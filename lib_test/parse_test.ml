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
open Patterns
open Patterns_lwt

let create () =
  let _ = Create_vhd.disk in
  ()

let diff () =
  let _ = Diff_vhd.disk in
  ()

let tmp_file_dir = "/tmp"
let disk_name_stem = tmp_file_dir ^ "/parse_test."
let disk_suffix = ".vhd"

let make_new_filename =
  let counter = ref 0 in
  fun () ->
    let this = !counter in
    incr counter;
    disk_name_stem ^ (string_of_int this) ^ disk_suffix

(* Create a dynamic disk, check headers *)
let check_empty_disk size =
  let filename = make_new_filename () in
  Vhd_IO.create_dynamic ~filename ~size () >>= fun vhd ->
  Vhd_IO.openfile filename false >>= fun vhd' ->
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
  Vhd_IO.openfile filename false >>= fun vhd'' ->
  assert_equal ~printer:Header.to_string ~cmp:Header.equal vhd'.Vhd.header vhd''.Vhd.header;
  assert_equal ~printer:Footer.to_string vhd'.Vhd.footer vhd''.Vhd.footer;
  assert_equal ~printer:BAT.to_string ~cmp:BAT.equal vhd'.Vhd.bat vhd''.Vhd.bat;
  Vhd_IO.close vhd'' >>= fun () ->
  Vhd_IO.close vhd' >>= fun () ->
  Vhd_IO.close vhd

(* Check ../ works in parent locator *)
let check_parent_parent_dir () =
  let filename = make_new_filename () in
  Vhd_IO.create_dynamic ~filename ~size:0L () >>= fun vhd ->
  let leaf_path = Filename.(concat (concat tmp_file_dir "leaves") "leaf.vhd") in
  let leaf_dir = Filename.dirname leaf_path in
  (try Unix.mkdir leaf_dir 0o0644 with _ -> ());
  Vhd_IO.create_difference ~filename:leaf_path ~parent:vhd ~relative_path:true () >>= fun vhd' ->
  (* Make sure we can open the leaf *)
  Vhd_IO.openfile leaf_path false >>= fun vhd'' ->
  Vhd_IO.close vhd'' >>= fun () ->
  Vhd_IO.close vhd' >>= fun () ->
  Vhd_IO.close vhd

(* Check we respect RO-ness *)
let check_readonly () =
  let filename = make_new_filename () in
  Vhd_IO.create_dynamic ~filename ~size:0L () >>= fun vhd ->
  Vhd_IO.close vhd >>= fun () ->
  Unix.chmod filename 0o400;
  Vhd_IO.openfile filename true >>= fun vhd ->
  Vhd_IO.close vhd

let fill_sector_with pattern =
  let b = Memory.alloc 512 in
  for i = 0 to 511 do
    Cstruct.set_char b i (pattern.[i mod (String.length pattern)])
  done;
  b

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

let sectors = Hashtbl.create 16
let sector_lookup message =
  if Hashtbl.mem sectors message
  then Hashtbl.find sectors message
  else
    let data = fill_sector_with message in
    Hashtbl.replace sectors message data;
    data

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
  | Write (position, message) ->
    let data = sector_lookup message in
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

let verify state = match state.child with
  | None -> return ()
  | Some t -> verify t state.contents

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

let all_program_tests = List.map (fun p ->
  (string_of_program p) >:: (fun () -> Lwt_main.run (run p))
) programs

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
      "check_parent_parent_dir" >:: (fun () -> Lwt_main.run (check_parent_parent_dir ()));
      "check_readonly" >:: (fun () -> Lwt_main.run (check_readonly ()));
     ] @ (List.map check_empty_disk sizes)
       @ (List.map check_empty_snapshot sizes)
       @ all_program_tests in
  run_test_tt ~verbose:!verbose suite

