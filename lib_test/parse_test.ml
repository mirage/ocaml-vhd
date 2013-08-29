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

let dynamic_disk_name = "/tmp/dynamic.vhd"

let sizes = [
  0L;
  4194304L;
  max_disk_size;
]

(* Create a dynamic disk, stream contents *)
let check_empty_disk size =
  lwt vhd = Vhd_IO.create_dynamic ~filename:dynamic_disk_name ~size () in
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
  let block = match block with
  | First -> 0l
  | Last -> Int32.sub vhd.Vhd.header.Header.max_table_entries 1l in
  let sectors_per_block = 1 lsl vhd.Vhd.header.Header.block_size_sectors_shift in
  let relative_sector = match sector with
  | First -> 0
  | Last -> sectors_per_block - 1 in
  Int64.(add(mul (of_int32 block) (of_int sectors_per_block)) (of_int relative_sector))

let write_sector vhd p =
  let sector = absolute_sector_of vhd p in
  Vhd_IO.write_sector vhd sector nonzero_sector

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

(* Check writing and then reading back works *)
let check_read_write size p =
  lwt vhd = Vhd_IO.create_dynamic ~filename:dynamic_disk_name ~size () in
  let sector = absolute_sector_of vhd p in
  lwt () = Vhd_IO.write_sector vhd sector nonzero_sector in
  lwt x = Vhd_IO.read_sector vhd sector in
  ( match x with
    | None -> failwith "read after write failed"
    | Some x ->
      assert_equal ~printer:cstruct_to_string ~cmp:cstruct_equal nonzero_sector x );
  Vhd_IO.close vhd
  

(* Check everything still works with a simple chain *)

(* ... with all data in the parent *)

(* ... with all data in the leaf *)

(* ... with data overwritten in the leaf *)

(* ... and all of that again with a larger leaf *)


let rec allpairs xs ys = match xs with
  | [] -> []
  | x :: xs -> List.map (fun y -> x, y) ys @ (allpairs xs ys)

let _ =
  let verbose = ref false in
  Arg.parse [
    "-verbose", Arg.Unit (fun _ -> verbose := true), "Run in verbose mode";
  ] (fun x -> Printf.fprintf stderr "Ignoring argument: %s" x)
    "Test vhd parser";

  let check_read_write (size, p) =
    Printf.sprintf "check_read_write_%Ld_%s_%s" size (string_of_choice p.block) (string_of_choice p.sector)
    >:: (fun () -> Lwt_main.run (check_read_write size p)) in
  let check_empty_disk size =
    Printf.sprintf "check_empty_disk_%Ld" size
    >:: (fun () -> Lwt_main.run (check_empty_disk size)) in

  let suite = "vhd" >:::
    [
      "create" >:: create;
    ] @ (List.map check_empty_disk sizes)
      @ (List.map check_read_write (allpairs sizes positions)) in
  run_test_tt ~verbose:!verbose suite

