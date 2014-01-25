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

module Impl = Vhd.F.From_file(IO)
open Vhd.F
open IO
open Impl
open Patterns

module Memory = struct
  let alloc bytes =
    if bytes = 0
    then Cstruct.create 0
    else
      let n = max 1 ((bytes + 4095) / 4096) in
      let pages = Io_page.(to_cstruct (get n)) in
      Cstruct.sub pages 0 bytes
end

let disk_name_stem = "/tmp/dynamic."
let disk_suffix = ".vhd"

let make_new_filename =
  let counter = ref 0 in
  fun () ->
    let this = !counter in
    incr counter;
    disk_name_stem ^ (string_of_int this) ^ disk_suffix

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
    | `Empty y -> 
     (* all sectors in [offset, offset + y = 1] should not be in the contents list *)
      List.iter (fun (x, _) ->
        if x >= offset && x < (Int64.add offset y)
        then failwith (Printf.sprintf "Sector %Ld is not supposed to be empty" x)
      ) expected;
      return (Int64.add offset y)
    | `Copy(handle, offset', len) ->
      (* all sectors in [offset, offset + len - 1] should be in the contents list *)
      (* XXX: this won't cope with very large copy requests *)
      let data = Memory.alloc (Int64.to_int len * 512) in
      really_read handle (Int64.(mul offset' 512L)) data >>= fun () ->
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
    | `Sectors data ->
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

let verify t contents =
    let capacity = Int64.(shift_left (of_int t.Vhd.header.Header.max_table_entries) (t.Vhd.header.Header.block_size_sectors_shift + sector_shift)) in
    ( if capacity < t.Vhd.footer.Footer.current_size
      then fail (Failure (Printf.sprintf "insufficient capacity in vhd: max table entries = %d; capacity = %Ld; current_size = %Ld" t.Vhd.header.Header.max_table_entries capacity t.Vhd.footer.Footer.current_size))
      else return () ) >>= fun () ->

    check_written_sectors t contents >>= fun () ->
    check_raw_stream_contents ~allow_empty:false t contents >>= fun () ->

    let write_stream fd stream =
      fold_left (fun offset x -> match x with
        | `Empty y -> return (Int64.(add offset (mul y 512L)))
        | `Sectors data ->
          really_write fd offset data >>= fun () ->
          return (Int64.(add offset (of_int (Cstruct.len data))))
        | `Copy(fd', offset', len') ->
          let buf = Memory.alloc (Int64.to_int len' * 512) in
          really_read fd' (Int64.mul offset' 512L) buf >>= fun () ->
          really_write fd offset buf >>= fun () ->
          return (Int64.(add offset (of_int (Cstruct.len buf))))
      ) 0L stream.elements in

    (* Stream the contents as a fresh vhd *)
    let filename = make_new_filename () in
    create filename >>= fun fd ->
    Vhd_input.vhd t >>= fun stream ->
    write_stream fd stream >>= fun _ ->
    close fd >>= fun () ->
    (* Check the contents look correct *)
    Vhd_IO.openchain filename false >>= fun t' ->
    check_written_sectors t' contents >>= fun () ->
    check_raw_stream_contents ~allow_empty:true t' contents >>= fun () ->
    Vhd_IO.close t' >>= fun () ->
    (* Stream the contents as a fresh vhd with a batmap *)
    let filename = make_new_filename () in
    create filename >>= fun fd ->
    Vhd_input.vhd ~emit_batmap:true t >>= fun stream ->
    write_stream fd stream >>= fun _ ->
    close fd >>= fun () ->
    (* Check the contents look correct *)
    Vhd_IO.openchain filename false >>= fun t' ->
    check_written_sectors t' contents >>= fun () ->
    check_raw_stream_contents ~allow_empty:true t' contents >>= fun () ->
    Vhd_IO.close t'


