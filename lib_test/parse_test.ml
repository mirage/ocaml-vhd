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
open Vhd
open Vhd_lwt

let create () =
  let _ = Create_vhd.disk in
  ()

let diff () =
  let _ = Diff_vhd.disk in
  ()

let dynamic_disk_name = "dynamic.vhd"

let test_sizes = [
(*
  0L;
*)
  4194304L;
(*
  Vhd.max_disk_size;
*)
]

(* Create a dynamic disk, stream contents *)
let check_empty_disk size =
  lwt vhd = Vhd_IO.create_dynamic ~filename:dynamic_disk_name ~size:4194304L () in
  lwt vhd' = Vhd_IO.openfile dynamic_disk_name in
  assert_equal ~printer:Header.to_string vhd.Vhd.header vhd'.Vhd.header;
  assert_equal ~printer:Footer.to_string vhd.Vhd.footer vhd'.Vhd.footer;
  assert_equal ~printer:BAT.to_string vhd.Vhd.bat vhd'.Vhd.bat;
  lwt () = Vhd_IO.close vhd' in
  Vhd_IO.close vhd

(* Look for problems reading and writing to edge-cases *)
type choice =
  | First
  | Last

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

(* Check everything still works with a simple chain *)

(* ... with all data in the parent *)

(* ... with all data in the leaf *)

(* ... with data overwritten in the leaf *)

(* ... and all of that again with a larger leaf *)


let _ =
  let verbose = ref false in
  Arg.parse [
    "-verbose", Arg.Unit (fun _ -> verbose := true), "Run in verbose mode";
  ] (fun x -> Printf.fprintf stderr "Ignoring argument: %s" x)
    "Test vhd parser";

  let suite = "vhd" >:::
    [
      "create" >:: create;
      "check_empty_disk" >:: (fun () -> Lwt_main.run (check_empty_disk 0L));
    ] in
  run_test_tt ~verbose:!verbose suite

