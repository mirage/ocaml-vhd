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

  let really_read_into { fd; filename; lock } offset (* in file *) buf =
    assert (buf.Cstruct.off = 0);
    let n = buf.Cstruct.len in
    let buf = buf.Cstruct.buffer in
    let rec rread acc fd buf ofs len = 
      lwt n = Lwt_bytes.read fd buf ofs len in
      let len = len - n in
      let acc = acc + n in
      if len = 0 || n = 0
      then return acc
      else rread acc fd buf (ofs + n) len in
    (* All reads and writes should be sector-aligned *)
    assert(Int64.(mul(div offset 512L) 512L) = offset);
    if (n / 512 * 512 <> n) then begin
      Printf.fprintf stderr "not sector aligned: %d\n%!" n;
      assert false
    end;
    assert(n / 512 * 512 = n);

    Lwt_mutex.with_lock lock
      (fun () ->
        try_lwt
          lwt _ = Lwt_unix.LargeFile.lseek fd offset Unix.SEEK_SET in
          lwt read = rread 0 fd buf 0 n in
          if read = 0 && n <> 0
          then fail End_of_file
          else return (Cstruct.(sub (of_bigarray buf) 0 n))
        with
        | Unix.Unix_error(Unix.EINVAL, "read", "") as e ->
          Printf.fprintf stderr "really_read offset = %Ld len = %d: EINVAL (alignment?)\n%!" offset n;
          fail e
        | End_of_file as e ->
          Printf.fprintf stderr "really_read offset = %Ld len = %d: End_of_file\n%!" offset n;
          fail e 
      )

  let really_read fd offset n =
    let buf = Memory.alloc n in
    really_read_into fd offset buf

  let really_write { fd; filename; lock } offset (* in file *) buffer =
    let ofs = buffer.Cstruct.off in
    let len = buffer.Cstruct.len in
    let buf = buffer.Cstruct.buffer in
    (* All reads and writes should be sector-aligned *)
    assert(Int64.(mul(div offset 512L) 512L) = offset);
    assert(len / 512 * 512 = len);

    let rec rwrite acc fd buf ofs len =
      lwt n = Lwt_bytes.write fd buf ofs len in
      let len = len - n in
      let acc = acc + n in
      if len = 0 || n = 0
      then return acc
      else rwrite acc fd buf (ofs + n) len in
    Lwt_mutex.with_lock lock
      (fun () ->
        try_lwt
          lwt _ = Lwt_unix.LargeFile.lseek fd offset Unix.SEEK_SET in
          lwt written = rwrite 0 fd buf ofs len in
          if written = 0 && len <> 0
          then fail End_of_file
          else return ()
        with
        | Unix.Unix_error(Unix.EINVAL, "write", "") as e ->
          Printf.fprintf stderr "really_write offset = %Ld len = %d: EINVAL (alignment?)\n%!" offset len;
          fail e
        | End_of_file as e ->
          Printf.fprintf stderr "really_write offset = %Ld len = %d: End_of_file\n%!" offset (Cstruct.len buffer);
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

module Impl = Vhd.Make(IO)
include Impl
include Fd 
