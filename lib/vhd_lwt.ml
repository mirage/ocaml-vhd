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
  let fail = Lwt.fail

  type fd = Cstruct.t

  let exists path = return (try ignore(Unix.stat path); true with _ -> false)

  let open_create_common flags filename =
    lwt fd = Lwt_unix.openfile filename flags 0o664 in
    let mmap = Cstruct.of_bigarray (Lwt_bytes.map_file ~fd:(Lwt_unix.unix_file_descr fd) ~shared:true ()) in
    return mmap

  let openfile = open_create_common [ Unix.O_RDWR ]
  let create = open_create_common [ Unix.O_RDWR; Unix.O_CREAT ]

  let close _ = return () (* the finaliser of the underlying Bigarray calls munmap(2) *)

  let y2k = 946684800.0 (* seconds from the unix epoch to the vhd epoch *)

  let get_vhd_time time =
    Int32.of_int (int_of_float (time -. y2k))

  let now () =
    let time = Unix.time() in
    get_vhd_time time

  let get_modification_time x =
    let st = Unix.stat x in
    return (get_vhd_time (st.Unix.st_mtime))

  let really_read mmap pos n = 
    let buf = Cstruct.sub mmap (Int64.to_int pos) n in
    return buf

  let really_write mmap pos x =
    let buf = Cstruct.sub mmap (Int64.to_int pos) (Cstruct.len x) in
    Cstruct.blit x 0 buf 0 (Cstruct.len x);
    return ()
end

open Vhd

module Impl = Make(MMAP)
include Impl
include MMAP

(* FIXME: This function does not do what it says! *)
let utf16_of_utf8 string =
  Array.init (String.length string) 
    (fun c -> int_of_char string.[c])

