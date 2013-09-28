(*
 * Copyright (C) 2011-2013 Citrix Inc
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)
open OUnit
open Lwt

module Impl = Vhd.Make(Vhd_lwt)
open Impl
open Vhd
open Vhd_lwt

let create () =
  let _ = Create_vhd.disk in
  ()

let diff () =
  let _ = Diff_vhd.disk in
  ()

let disk_name_stem = "/tmp/dynamic."
let disk_suffix = ".vhd"

let make_new_filename =
  let counter = ref 0 in
  fun () ->
    let this = !counter in
    incr counter;
    disk_name_stem ^ (string_of_int this) ^ disk_suffix

let sizes = [
  0L;
  4194304L;
  max_disk_size;
]

(* Create a dynamic disk, check headers *)
let check_empty_disk size =
  let filename = make_new_filename () in
  Vhd_IO.create_dynamic ~filename ~size () >>= fun vhd ->
  Vhd_IO.openfile filename >>= fun vhd' ->
  assert_equal ~printer:Header.to_string ~cmp:Header.equal vhd.Vhd.header vhd'.Vhd.header;
  assert_equal ~printer:Footer.to_string vhd.Vhd.footer vhd'.Vhd.footer;
  assert_equal ~printer:BAT.to_string ~cmp:BAT.equal vhd.Vhd.bat vhd'.Vhd.bat;
  Vhd_IO.close vhd' >>= fun () ->
  Vhd_IO.close vhd

(* Create a snapshot, check headers *)
let check_empty_snapshot size =
  let filename = make_new_filename () in
  Vhd_IO.create_dynamic ~filename ~size () >>= fun vhd ->
  let filename = make_new_filename () in
  Vhd_IO.create_difference ~filename ~parent:vhd () >>= fun vhd' ->
  Vhd_IO.openfile filename >>= fun vhd'' ->
  assert_equal ~printer:Header.to_string ~cmp:Header.equal vhd'.Vhd.header vhd''.Vhd.header;
  assert_equal ~printer:Footer.to_string vhd'.Vhd.footer vhd''.Vhd.footer;
  assert_equal ~printer:BAT.to_string ~cmp:BAT.equal vhd'.Vhd.bat vhd''.Vhd.bat;
  Vhd_IO.close vhd'' >>= fun () ->
  Vhd_IO.close vhd' >>= fun () ->
  Vhd_IO.close vhd

(* Look for problems reading and writing to edge-cases *)
type choice =
  | First
  | Last

let string_of_choice = function
  | First -> "first"
  | Last -> "last"

type position = {
  block: choice;
  sector: choice;
}

let positions = [
  { block = First; sector = First };
  { block = First; sector = Last };
  { block = Last; sector = First };
  { block = Last; sector = Last }
]

let fill_sector_with pattern =
  let b = Memory.alloc 512 in
  for i = 0 to 511 do
    Cstruct.set_char b i (pattern.[i mod (String.length pattern)])
  done;
  b

let first_write_message = "This is a sector which contains simple data.\n"
let second_write_message = "All work and no play makes Dave a dull boy.\n"
let first_sector = fill_sector_with first_write_message
let second_sector = fill_sector_with second_write_message

let absolute_sector_of vhd { block; sector } =
  if vhd.Vhd.header.Header.max_table_entries = 0
  then None
  else
    let block = match block with
    | First -> 0
    | Last -> vhd.Vhd.header.Header.max_table_entries - 1 in
    let sectors_per_block = 1 lsl vhd.Vhd.header.Header.block_size_sectors_shift in
    let relative_sector = match sector with
    | First -> 0
    | Last -> sectors_per_block - 1 in
    Some (Int64.(add(mul (of_int block) (of_int sectors_per_block)) (of_int relative_sector)))

let cstruct_to_string c = String.escaped (Cstruct.to_string c)

type operation =
  | Create of int64
  | Snapshot
  | Write of (position * string * Cstruct.t)

let descr_of_operation = function
  | Create x -> Printf.sprintf "create a disk of size %Ld bytes" x
  | Snapshot -> "take a snapshot"
  | Write (p, message, _) -> Printf.sprintf "write \"%s\"to the %s sector of the %s block" (String.escaped message) (string_of_choice p.sector) (string_of_choice p.block)

let string_of_operation = function
  | Create x -> Printf.sprintf "Create:%Ld" x
  | Snapshot -> "Snapshot"
  | Write (p, _, _) -> Printf.sprintf "Write:%s:%s" (string_of_choice p.block) (string_of_choice p.sector)

let descr_of_program = function
  | [] -> ""
  | [ x ] -> descr_of_operation x
  | x :: xs -> "First, " ^ (descr_of_operation x) ^ "; then " ^ (String.concat "; then " (List.map descr_of_operation xs))

let string_of_program p = String.concat "_" (List.map string_of_operation p)

type state = {
  to_close: fd Vhd.t list;
  to_unlink: string list;
  child: fd Vhd.t option;
  contents: (int64 * Cstruct.t) list;
}

let initial = {
  to_close = [];
  to_unlink = [];
  child = None;
  contents = [];
}

let execute state = function
  | Create size ->
    let filename = make_new_filename () in
    Vhd_IO.create_dynamic ~filename ~size () >>= fun vhd ->
    return {
      to_close = vhd :: state.to_close;
      to_unlink = filename :: state.to_unlink;
      child = Some vhd;
      contents = [];
    }
  | Snapshot ->
    let vhd = match state.child with
    | Some vhd -> vhd
    | None -> failwith "no vhd open" in
    let filename = make_new_filename () in
    Vhd_IO.create_difference ~filename ~parent:vhd () >>= fun vhd' ->
    return {
      to_close = vhd' :: state.to_close;
      to_unlink = filename :: state.to_unlink;
      child = Some vhd';
      contents = state.contents;
    }
  | Write (position, _, data) ->
    let vhd = match state.child with
    | Some vhd -> vhd
    | None -> failwith "no vhd open" in
    begin match absolute_sector_of vhd position with
      | Some sector ->
        Vhd_IO.write_sector vhd sector data >>= fun () ->
        (* Overwrite means we forget any previous contents *)
        let contents = List.filter (fun (x, _) -> x <> sector) state.contents in
        return { state with contents = (sector, data) :: contents }
      | None ->
        return state
    end

(* Verify that vhd [t] contains the sectors [expected] *)
let rec check_written_sectors t expected = match expected with
  | [] -> return ()
  | (x, data) :: xs ->
    Vhd_IO.read_sector t x >>= fun y ->
    ( match y with
    | None -> fail (Failure "read after write failed")
    | Some y ->
      assert_equal ~printer:cstruct_to_string ~cmp:cstruct_equal data y;
      return () ) >>= fun () ->
    check_written_sectors t xs

let empty_sector = Memory.alloc 512

(* Verify the raw data stream from [t] contains exactly [expected] and no more.
   If ~allow_empty then we accept sectors which are present (in the bitmap) but
   physically empty. *)
let check_raw_stream_contents ~allow_empty t expected =
  Vhd_input.raw t >>= fun stream ->
  fold_left (fun offset x -> match x with
    | Element.Empty y -> 
     (* all sectors in [offset, offset + y = 1] should not be in the contents list *)
      List.iter (fun (x, _) ->
        if x >= offset && x < (Int64.add offset y)
        then failwith (Printf.sprintf "Sector %Ld is not supposed to be empty" x)
      ) expected;
      return (Int64.add offset y)
    | Element.Copy(handle, offset', len) ->
      (* all sectors in [offset, offset + len - 1] should be in the contents list *)
      (* XXX: this won't cope with very large copy requests *)
      Fd.really_read handle (Int64.(mul offset' 512L)) (Int64.to_int len * 512) >>= fun data ->
      let rec check i =
        if i >= (Int64.to_int len) then ()
        else
          let sector = Int64.(add offset (of_int i)) in
          let actual = Cstruct.sub data (i * 512) 512 in

          if not(List.mem_assoc sector expected) then begin
            if not allow_empty
            then failwith (Printf.sprintf "Sector %Ld is not supposed to be written to" sector)
            else assert_equal ~printer:cstruct_to_string ~cmp:cstruct_equal empty_sector actual
          end else begin
            let expected = List.assoc sector expected in
            assert_equal ~printer:cstruct_to_string ~cmp:cstruct_equal expected actual;
          end;
          check (i + 1) in
      check 0;
      return (Int64.(add offset len))
    | Element.Sectors data ->
      let rec loop offset remaining =
        if Cstruct.len remaining = 0
        then return offset
        else
          (* the sector [offset] should be in the contents list *)
          if not(List.mem_assoc offset expected)
          then failwith (Printf.sprintf "Sector %Ld is not supposed to be written to" offset)
          else
            let expected = List.assoc offset expected in
            let actual = Cstruct.sub remaining 0 sector_size in
            assert_equal ~printer:cstruct_to_string ~cmp:cstruct_equal expected actual;
            loop (Int64.(add offset 1L)) (Cstruct.shift remaining sector_size) in
      loop offset data
  ) 0L stream.elements >>= fun next_sector ->
  (* [next_sector] should be higher than the highest sector in the contents list *)
  let highest_sector = List.fold_left max (-1L) (List.map fst expected) in
  assert (next_sector > highest_sector);
  return ()

let verify state = match state.child with
  | None -> return ()
  | Some t ->
    let capacity = Int64.(shift_left (of_int t.Vhd.header.Header.max_table_entries) (t.Vhd.header.Header.block_size_sectors_shift + sector_shift)) in
    ( if capacity < t.Vhd.footer.Footer.current_size
      then fail (Failure (Printf.sprintf "insufficient capacity in vhd: max table entries = %d; capacity = %Ld; current_size = %Ld" t.Vhd.header.Header.max_table_entries capacity t.Vhd.footer.Footer.current_size))
      else return () ) >>= fun () ->

    check_written_sectors t state.contents >>= fun () ->
    check_raw_stream_contents ~allow_empty:false t state.contents >>= fun () ->

    let write_stream fd stream =
      fold_left (fun offset x -> match x with
        | Element.Empty y -> return (Int64.(add offset (mul y 512L)))
        | Element.Sectors data ->
          Fd.really_write fd offset data >>= fun () ->
          return (Int64.(add offset (of_int (Cstruct.len data))))
        | Element.Copy(fd', offset', len') ->
          really_read fd' (Int64.mul offset' 512L) (Int64.to_int len' * 512) >>= fun buf ->
          Fd.really_write fd offset buf >>= fun () ->
          return (Int64.(add offset (of_int (Cstruct.len buf))))
      ) 0L stream.elements in

    (* Stream the contents as a fresh vhd *)
    let filename = make_new_filename () in
    Fd.create filename >>= fun fd ->
    Vhd_input.vhd t >>= fun stream ->
    write_stream fd stream >>= fun _ ->
    Fd.close fd >>= fun () ->
    (* Check the contents look correct *)
    Vhd_IO.openfile filename >>= fun t' ->
    check_written_sectors t' state.contents >>= fun () ->
    check_raw_stream_contents ~allow_empty:true t' state.contents >>= fun () ->
    Vhd_IO.close t' >>= fun () ->
    (* Stream the contents as a fresh vhd with a batmap *)
    let filename = make_new_filename () in
    Fd.create filename >>= fun fd ->
    Vhd_input.vhd ~emit_batmap:true t >>= fun stream ->
    write_stream fd stream >>= fun _ ->
    Fd.close fd >>= fun () ->
    (* Check the contents look correct *)
    Vhd_IO.openfile filename >>= fun t' ->
    check_written_sectors t' state.contents >>= fun () ->
    check_raw_stream_contents ~allow_empty:true t' state.contents >>= fun () ->
    Vhd_IO.close t'


let cleanup state =
  List.iter Unix.unlink state.to_unlink;
  Lwt_list.iter_s Vhd_IO.close state.to_close

let run program =
  let single_instruction state x =
    execute state x >>= fun state' ->
    verify state' >>= fun () ->
    return state' in
  Lwt_list.fold_left_s single_instruction initial program >>= fun final_state ->
  cleanup final_state

let rec allpairs xs ys = match xs with
  | [] -> []
  | x :: xs -> List.map (fun y -> x, y) ys @ (allpairs xs ys)

let first_write p = Write(p, first_write_message, first_sector)
let second_write p = Write(p, second_write_message, second_sector)

(* Check writing and then reading back works *)
let create_write_read =
  List.map (fun (size, p) ->
    [ Create size; first_write p ]
  ) (allpairs sizes positions)

(* Check writing and then reading back works in a simple chain *)
let create_write_read_leaf =
  List.map (fun (size, p) ->
    [ Create size; Snapshot; first_write p ]
  ) (allpairs sizes positions)

(* Check writing and then reading back works in a chain where the writes are in the parent *)
let create_write_read_parent =
  List.map (fun (size, p) ->
    [ Create size; first_write p; Snapshot ]
  ) (allpairs sizes positions)

(* Check writing and then reading back works in a chain where there are writes in both parent and leaf *)
let create_write_overwrite =
  List.map (fun (size, (p1, p2)) ->
    [ Create size; first_write p1; Snapshot; second_write p2 ]
  ) (allpairs sizes (allpairs positions positions))

(* TODO: ... and all of that again with a larger leaf *)

let all_programs =
  List.concat [
    create_write_read;
    create_write_read_leaf;
    create_write_read_parent;
    create_write_overwrite;
]

let all_program_tests = List.map (fun p ->
  (string_of_program p) >:: (fun () -> Lwt_main.run (run p))
) all_programs

let _ =
  let verbose = ref false in
  Arg.parse [
    "-verbose", Arg.Unit (fun _ -> verbose := true), "Run in verbose mode";
  ] (fun x -> Printf.fprintf stderr "Ignoring argument: %s" x)
    "Test vhd parser";

  let check_empty_disk size =
    Printf.sprintf "check_empty_disk_%Ld" size
    >:: (fun () -> Lwt_main.run (check_empty_disk size)) in

  let check_empty_snapshot size =
    Printf.sprintf "check_empty_snapshot_%Ld" size
    >:: (fun () -> Lwt_main.run (check_empty_snapshot size)) in

  let suite = "vhd" >:::
    [
      "create" >:: create;
     ] @ (List.map check_empty_disk sizes)
       @ (List.map check_empty_snapshot sizes)
       @ all_program_tests in
  run_test_tt ~verbose:!verbose suite

