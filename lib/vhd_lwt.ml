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

module Fd = struct
  open Lwt

  type fd = {
    fd: Lwt_unix.file_descr;
    filename: string;
    lock: Lwt_mutex.t;
  }

  let openfile filename =
    let unix_fd = File.openfile filename 0o644 in
    let fd = Lwt_unix.of_unix_file_descr unix_fd in
    let lock = Lwt_mutex.create () in
    return { fd; filename; lock }

  let fsync { fd = fd } =
    let fd' = Lwt_unix.unix_file_descr fd in
    File.fsync fd'

  let size_of_file t =
    lwt s = Lwt_unix.LargeFile.fstat t.fd in
    return s.Lwt_unix.LargeFile.st_size

  let create filename =
    (* First create the file as normal *)
    lwt fd = Lwt_unix.openfile filename [ Unix.O_RDWR; Unix.O_CREAT; Unix.O_TRUNC ] 0o644 in
    lwt () = Lwt_unix.close fd in
    (* Then re-open using our new API *)
    openfile filename

  let close t = Lwt_unix.close t.fd

  exception Not_sector_aligned of int64

  let assert_sector_aligned n =
    if Int64.(mul(div n 512L) 512L) <> n then raise (Not_sector_aligned n)

  let really_read_into { fd; filename; lock } offset (* in file *) buf =
    let rec rread acc fd buf =
      lwt n = Lwt_cstruct.read fd buf in
      let acc = acc + n in
      let buf = Cstruct.shift buf n in
      if Cstruct.len buf = 0 || n = 0
      then return acc
      else rread acc fd buf in

    (* All reads and writes should be sector-aligned *)
    assert_sector_aligned offset;
    assert_sector_aligned (Int64.of_int buf.Cstruct.off);
    assert_sector_aligned (Int64.of_int (Cstruct.len buf));

    Lwt_mutex.with_lock lock
      (fun () ->
        try_lwt
          lwt _ = Lwt_unix.LargeFile.lseek fd offset Unix.SEEK_SET in
          lwt read = rread 0 fd buf in
          if read = 0 && (Cstruct.len buf <> 0)
          then fail End_of_file
          else return (Cstruct.(sub buf 0 read))
        with
        | Unix.Unix_error(Unix.EINVAL, "read", "") as e ->
          Printf.fprintf stderr "really_read offset = %Ld len = %d: EINVAL (alignment?)\n%!" offset (Cstruct.len buf);
          fail e
        | End_of_file as e ->
          Printf.fprintf stderr "really_read offset = %Ld len = %d: End_of_file\n%!" offset (Cstruct.len buf);
          fail e 
      )

  let really_read fd offset n =
    let buf = Memory.alloc n in
    really_read_into fd offset buf

  let really_write { fd; filename; lock } offset (* in file *) buf =
    let rec rwrite acc fd buf =
      lwt n = Lwt_cstruct.write fd buf in
      let buf = Cstruct.shift buf n in
      let acc = acc + n in
      if Cstruct.len buf = 0 || n = 0
      then return acc
      else rwrite acc fd buf in

    (* All reads and writes should be sector-aligned *)
    assert_sector_aligned offset;
    assert_sector_aligned (Int64.of_int buf.Cstruct.off);
    assert_sector_aligned (Int64.of_int (Cstruct.len buf));

    Lwt_mutex.with_lock lock
      (fun () ->
        try_lwt
          lwt _ = Lwt_unix.LargeFile.lseek fd offset Unix.SEEK_SET in
          lwt written = rwrite 0 fd buf in
          if written = 0 && Cstruct.len buf <> 0
          then fail End_of_file
          else return ()
        with
        | Unix.Unix_error(Unix.EINVAL, "write", "") as e ->
          Printf.fprintf stderr "really_write offset = %Ld len = %d: EINVAL (alignment?)\n%!" offset (Cstruct.len buf);
          fail e
        | End_of_file as e ->
          Printf.fprintf stderr "really_write offset = %Ld len = %d: End_of_file\n%!" offset (Cstruct.len buf);
          fail e 
      )
end

module IO = struct
  type 'a t = 'a Lwt.t

  let (>>=) = Lwt.(>>=)
  let return = Lwt.return
  let fail = Lwt.fail

  let exists path = return (try ignore(Unix.stat path); true with _ -> false)

  let y2k = 946684800.0 (* seconds from the unix epoch to the vhd epoch *)

  let get_vhd_time time =
    Int32.of_int (int_of_float (time -. y2k))

  let now () =
    let time = Unix.time() in
    get_vhd_time time

  let get_modification_time x =
    lwt st = Lwt_unix.LargeFile.stat x in
    return (get_vhd_time (st.Lwt_unix.LargeFile.st_mtime))

  let get_file_size x =
    lwt st = Lwt_unix.LargeFile.stat x in
    return st.Lwt_unix.LargeFile.st_size

  include Fd
  include Memory
end

include IO
