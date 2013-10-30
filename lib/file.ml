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

let use_unbuffered = ref false

external openfile_unbuffered: string -> int -> Unix.file_descr = "stub_openfile_direct"

let openfile_buffered filename mode = Unix.openfile filename [ Unix.O_RDWR ] mode

let openfile filename mode =
  (if !use_unbuffered then openfile_unbuffered else openfile_buffered) filename mode

external blkgetsize64: string -> int64 = "stub_blkgetsize64"

let get_file_size x =
    let st = Unix.LargeFile.stat x in
    match st.Unix.LargeFile.st_kind with
    | Unix.S_REG -> st.Unix.LargeFile.st_size
    | Unix.S_BLK -> blkgetsize64 x
    | _ -> failwith (Printf.sprintf "get_file_size: %s not a file or block device" x)


external fsync : Unix.file_descr -> unit = "stub_fsync"

