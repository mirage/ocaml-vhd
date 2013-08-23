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

module MMAP = struct
  type 'a t = 'a Lwt.t

  let (>>=) = Lwt.(>>=)
  let return = Lwt.return

  type fd = Cstruct.t

  let exists path = return (try ignore(Unix.stat path); true with _ -> false)

  let openfile filename =
    lwt fd = Lwt_unix.openfile filename [Unix.O_RDWR] 0o664 in
    let mmap = Cstruct.of_bigarray (Lwt_bytes.map_file ~fd:(Lwt_unix.unix_file_descr fd) ~shared:true ()) in
    return mmap

  let really_read mmap pos n = 
    let buf = Cstruct.sub mmap pos n in
    return buf

  let really_write mmap pos x =
    let buf = Cstruct.sub mmap pos (Cstruct.len x) in
    Cstruct.blit x 0 buf 0 (Cstruct.len x);
    return ()
end

open Vhd

module Impl = Make(MMAP)
include Impl
include MMAP

let blank_uuid = match Uuidm.of_bytes (String.make 16 '\000') with
  | Some x -> x
  | None -> assert false (* never happens *)

let y2k = 946684800.0 (* seconds from the unix epoch to the vhd epoch *)

let get_vhd_time time =
  Int32.of_int (int_of_float (time -. y2k))

let get_now () =
  let time = Unix.time() in
  get_vhd_time time

let get_parent_modification_time parent =
  let st = Unix.stat parent in
  get_vhd_time (st.Unix.st_mtime)

(* Create a completely new sparse VHD file *)
let create_new_dynamic filename requested_size uuid ?(sparse=true) ?(table_offset=2048L) 
    ?(block_size=Header.default_block_size) ?(data_offset=512L) ?(saved_state=false)
    ?(features=[Feature.Temporary]) () =

  (* Round up to the nearest 2-meg block *)
  let size = Int64.mul (Int64.div (Int64.add 2097151L requested_size) 2097152L) 2097152L in

  let geometry = Geometry.of_sectors (Int64.(to_int (div size (of_int sector_size)))) in
  let creator_application = Footer.default_creator_application in
  let creator_version = Footer.default_creator_version in
  let footer = 
    {
      Footer.features;
      data_offset;
      time_stamp = 0l;
      creator_application; creator_version;
      creator_host_os = Host_OS.Other 0l;
      original_size = size;
      current_size = size;
      geometry;
      disk_type = Disk_type.Dynamic_hard_disk;
      checksum = 0l; (* Filled in later *)
      uid = uuid;
      saved_state;
    }
  in
  let header = 
    {
      Header.table_offset; (* Stick the BAT at this offset *)
      max_table_entries = Int64.to_int32 (Int64.div size (Int64.of_int32 block_size));
      block_size;
      checksum = 0l;
      parent_unique_id = blank_uuid;
      parent_time_stamp = 0l;
      parent_unicode_name = [| |];
      parent_locators = Array.make 8 Parent_locator.null
    } 
  in
  let bat = Array.make (Int32.to_int header.Header.max_table_entries) (BAT.unused) in
  Printf.printf "max_table_entries: %ld (size=%Ld)\n" (header.Header.max_table_entries) size;
  lwt fd = Lwt_unix.openfile filename [Unix.O_RDWR; Unix.O_CREAT; Unix.O_EXCL] 0o640 in
  let mmap = Cstruct.of_bigarray (Lwt_bytes.map_file ~fd:(Lwt_unix.unix_file_descr fd) ~shared:true ~size:(1024*1024*64) ()) in
  Lwt.return {Vhd.filename=filename;
   handle=mmap;
   header=header;
   footer=footer;
   parent=None;
   bat=bat}

(* FIXME: This function does not do what it says! *)
let utf16_of_utf8 string =
  Array.init (String.length string) 
    (fun c -> int_of_char string.[c])

let create_new_difference filename backing_vhd uuid ?(features=[])
    ?(data_offset=512L) ?(saved_state=false) ?(table_offset=2048L) () =
  lwt parent = Vhd_IO.openfile backing_vhd in
  let creator_application = Footer.default_creator_application in
  let creator_version = Footer.default_creator_version in
  let footer = 
    {
      Footer.features;
      data_offset;
      time_stamp = get_now ();
      creator_application; creator_version;
      creator_host_os = Host_OS.Other 0l;
      original_size = parent.Vhd.footer.Footer.current_size;
      current_size = parent.Vhd.footer.Footer.current_size;
      geometry = parent.Vhd.footer.Footer.geometry;
      disk_type = Disk_type.Differencing_hard_disk;
      checksum = 0l;
      uid = uuid;
      saved_state = saved_state;
    }
  in
  let locator0 = 
    let uri = "file://./" ^ (Filename.basename backing_vhd) in
    let platform_data = Cstruct.create (String.length uri) in
    Cstruct.blit_from_string uri 0 platform_data 0 (String.length uri);
    {
      Parent_locator.platform_code = Platform_code.MacX;
      platform_data_space = 1l;
      platform_data_space_original=1l;
      platform_data_length = Int32.of_int (String.length uri);
      platform_data_offset = 1536L;
      platform_data;
    }
  in
  let header = 
    {
      Header.table_offset;
      max_table_entries = parent.Vhd.header.Header.max_table_entries;
      block_size = parent.Vhd.header.Header.block_size;
      checksum = 0l;
      parent_unique_id = parent.Vhd.footer.Footer.uid;
      parent_time_stamp = get_parent_modification_time backing_vhd;
      parent_unicode_name = utf16_of_utf8 backing_vhd;
      parent_locators = [| locator0; Parent_locator.null; Parent_locator.null; Parent_locator.null;
			     Parent_locator.null; Parent_locator.null; Parent_locator.null; Parent_locator.null; |];
    }
  in
  let bat = Array.make (Int32.to_int header.Header.max_table_entries) (BAT.unused) in
  lwt fd = Lwt_unix.openfile filename [Unix.O_RDWR; Unix.O_CREAT; Unix.O_EXCL] 0o640 in
  let mmap = Cstruct.of_bigarray (Lwt_bytes.map_file ~fd:(Lwt_unix.unix_file_descr fd) ~shared:true () ~size:(1024*1024*64)) in
  Lwt.return {Vhd.filename=filename;
   handle=mmap;
   header=header;
   footer=footer;
   parent=Some parent;
   bat=bat}

