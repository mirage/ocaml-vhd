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
  lwt vhd = Vhd_IO.create_dynamic ~filename ~size () in
  lwt vhd' = Vhd_IO.openfile filename in
  assert_equal ~printer:Header.to_string vhd.Vhd.header vhd'.Vhd.header;
  assert_equal ~printer:Footer.to_string vhd.Vhd.footer vhd'.Vhd.footer;
  assert_equal ~printer:BAT.to_string vhd.Vhd.bat vhd'.Vhd.bat;
  lwt () = Vhd_IO.close vhd' in
  Vhd_IO.close vhd

(* Create a snapshot, check headers *)
let check_empty_snapshot size =
  let filename = make_new_filename () in
  lwt vhd = Vhd_IO.create_dynamic ~filename ~size () in
  let filename = make_new_filename () in
  lwt vhd' = Vhd_IO.create_difference ~filename ~parent:vhd () in
  lwt vhd'' = Vhd_IO.openfile filename in
  assert_equal ~printer:Header.to_string vhd'.Vhd.header vhd''.Vhd.header;
  assert_equal ~printer:Footer.to_string vhd'.Vhd.footer vhd''.Vhd.footer;
  assert_equal ~printer:BAT.to_string vhd'.Vhd.bat vhd''.Vhd.bat;
  lwt () = Vhd_IO.close vhd'' in
  lwt () = Vhd_IO.close vhd' in
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

let nonzero_sector =
  let b = Cstruct.create 512 in
  let pattern = "This is a sector which contains simple data.\n" in
  for i = 0 to 511 do
    Cstruct.set_char b i (pattern.[i mod (String.length pattern)])
  done;
  b

let absolute_sector_of vhd { block; sector } =
  if vhd.Vhd.header.Header.max_table_entries = 0l
  then None
  else
    let block = match block with
    | First -> 0l
    | Last -> Int32.sub vhd.Vhd.header.Header.max_table_entries 1l in
    let sectors_per_block = 1 lsl vhd.Vhd.header.Header.block_size_sectors_shift in
    let relative_sector = match sector with
    | First -> 0
    | Last -> sectors_per_block - 1 in
    Some (Int64.(add(mul (of_int32 block) (of_int sectors_per_block)) (of_int relative_sector)))

let cstruct_equal a b =
  let check_contents a b =
    try
      for i = 0 to Cstruct.len a - 1 do
        if Cstruct.get_char a i <> (Cstruct.get_char b i)
        then failwith "argh"
      done;
      true
    with _ -> false in
  (Cstruct.len a = (Cstruct.len b)) && (check_contents a b)

let cstruct_to_string c = String.escaped (Cstruct.to_string c)

type operation =
  | Create of int64
  | Snapshot
  | Write of position

let descr_of_operation = function
  | Create x -> Printf.sprintf "create a disk of size %Ld bytes" x
  | Snapshot -> "take a snapshot"
  | Write p -> Printf.sprintf "write to the %s sector of the %s block" (string_of_choice p.sector) (string_of_choice p.block)

let string_of_operation = function
  | Create x -> Printf.sprintf "Create:%Ld" x
  | Snapshot -> "Snapshot"
  | Write p -> Printf.sprintf "Write:%s:%s" (string_of_choice p.block) (string_of_choice p.sector)

let descr_of_program = function
  | [] -> ""
  | [ x ] -> descr_of_operation x
  | x :: xs -> "First, " ^ (descr_of_operation x) ^ "; then " ^ (String.concat "; then " (List.map descr_of_operation xs))

let string_of_program p = String.concat "_" (List.map string_of_operation p)

type state = {
  to_close: Vhd_IO.handle Vhd.t list;
  to_unlink: string list;
  child: Vhd_IO.handle Vhd.t option;
  contents: int64 list;
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
    lwt vhd = Vhd_IO.create_dynamic ~filename ~size () in
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
    lwt vhd' = Vhd_IO.create_difference ~filename ~parent:vhd () in
    return {
      to_close = vhd' :: state.to_close;
      to_unlink = filename :: state.to_unlink;
      child = Some vhd';
      contents = state.contents;
    }
  | Write position ->
    let vhd = match state.child with
    | Some vhd -> vhd
    | None -> failwith "no vhd open" in
    begin match absolute_sector_of vhd position with
      | Some sector ->
        lwt () = Vhd_IO.write_sector vhd sector nonzero_sector in
        return { state with contents = sector :: state.contents }
      | None ->
        return state
    end

let verify state = match state.child with
  | None -> return ()
  | Some vhd ->
    let rec loop = function
      | [] -> return ()
      | x :: xs ->
        lwt y = Vhd_IO.read_sector vhd x in
        lwt () = match y with
        | None -> fail (Failure "read after write failed")
        | Some y ->
          assert_equal ~printer:cstruct_to_string ~cmp:cstruct_equal nonzero_sector y;
          return () in
        loop xs in
    lwt () = loop state.contents in
    lwt stream = raw vhd in
    lwt next_sector = fold_left (fun offset x -> match x with
      | Element.Empty y ->
        (* all sectors in [offset, offset + y = 1] should not be in the contents list *)
        List.iter (fun x ->
          if x >= offset && x < (Int64.add offset y)
          then failwith (Printf.sprintf "Sector %Ld is not supposed to be empty" x)
        ) state.contents;
        return (Int64.add offset y)
      | Element.Copy(vhd', offset', len) ->
        (* all sectors in [offset, offset + len - 1] should be in the contents list *)
        for i = 0 to len - 1 do
          let sector = Int64.(add offset (of_int i)) in
          if not(List.mem sector state.contents)
          then failwith (Printf.sprintf "Sector %Ld is not supposed to be written to" sector)
        done;
        return (Int64.(add offset (of_int len)))
      | Element.Sector data ->
        (* the sector [offset] should be in the contents list *)
        if not(List.mem offset state.contents)
        then failwith (Printf.sprintf "Sector %Ld is not supposed to be written to" offset);
        return (Int64.(add offset 1L))
    ) 0L stream in
    (* [next_sector] should be higher than the highest sector in the contents list *)
    let highest_sector = List.fold_left max (-1L) state.contents in
    assert (next_sector > highest_sector);
    return ()

let cleanup state =
  List.iter Unix.unlink state.to_unlink;
  Lwt_list.iter_s Vhd_IO.close state.to_close

let run program =
  let single_instruction state x =
    lwt state' = execute state x in
    lwt () = verify state' in
    return state' in
  lwt final_state = Lwt_list.fold_left_s single_instruction initial program in
  cleanup final_state

let rec allpairs xs ys = match xs with
  | [] -> []
  | x :: xs -> List.map (fun y -> x, y) ys @ (allpairs xs ys)

(* Check writing and then reading back works *)
let create_write_read =
  List.map (fun (size, p) ->
    [ Create size; Write p ]
  ) (allpairs sizes positions)

(* Check writing and then reading back works in a simple chain *)
let create_write_read_leaf =
  List.map (fun (size, p) ->
    [ Create size; Snapshot; Write p ]
  ) (allpairs sizes positions)

(* Check writing and then reading back works in a chain where the writes are in the parent *)
let create_write_read_parent =
  List.map (fun (size, p) ->
    [ Create size; Write p; Snapshot ]
  ) (allpairs sizes positions)

(* Check writing and then reading back works in a chain where there are writes in both parent and leaf *)
let create_write_overwrite =
  List.map (fun (size, (p1, p2)) ->
    [ Create size; Write p1; Snapshot; Write p2 ]
  ) (allpairs sizes (allpairs positions positions))

(* TODO: ... and all of that again with a larger leaf *)

let all_programs =
  List.concat [
    create_write_read;
    create_write_read_leaf;
(*
    create_write_read_parent;
    create_write_overwrite;
*)
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

