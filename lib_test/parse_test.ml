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
  0L;
  4194304L;
(*
  Vhd.max_disk_size;
*)
]

(* Create a dynamic disk, stream contents *)
let check_empty_disk size =
  lwt vhd = Vhd_IO.create_dynamic ~filename:dynamic_disk_name ~size:4194304L () in
  Vhd_IO.close vhd

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

