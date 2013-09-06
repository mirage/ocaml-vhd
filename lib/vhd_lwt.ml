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

let use_odirect = ref true

module Memory = struct

  type buf = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

  external alloc_pages: int -> buf = "caml_alloc_pages"

  let get n =
    if n < 1
    then raise (Invalid_argument "The number of page should be greater or equal to 1")
    else
      try alloc_pages n with _ ->
        Gc.compact ();
        try alloc_pages n with _ -> raise Out_of_memory

  let page_size = 4096

  let alloc_bigarray bytes =
    (* round up to next PAGE_SIZE *)
    let pages = (bytes + page_size - 1) / page_size in
    (* but round-up 0 pages to 0 *)
    let pages = max pages 1 in
    get pages

  let alloc bytes =
    let larger_than_we_need = Cstruct.of_bigarray (alloc_bigarray bytes) in
    Cstruct.sub larger_than_we_need 0 bytes
end

module Fd = struct
  open Lwt

  type fd = {
    fd: Lwt_unix.file_descr;
    filename: string;
    lock: Lwt_mutex.t;
  }

  external openfile_direct: string -> int -> Unix.file_descr = "stub_openfile_direct"
  let openfile_buffered filename mode =
    Unix.openfile filename [ Unix.O_RDWR ] mode

  let openfile filename =
    let unix_fd = (if !use_odirect then openfile_direct else openfile_buffered) filename 0o644 in
    let fd = Lwt_unix.of_unix_file_descr unix_fd in
    let lock = Lwt_mutex.create () in
    return { fd; filename; lock }

  let create filename =
    (* First create the file as normal *)
    lwt fd = Lwt_unix.openfile filename [ Unix.O_RDWR; Unix.O_CREAT; Unix.O_TRUNC ] 0o644 in
    lwt () = Lwt_unix.close fd in
    (* Then re-open using our new API *)
    openfile filename

  let close t = Lwt_unix.close t.fd

  let really_read { fd; filename; lock } offset (* in file *) n =
    let buf = Memory.alloc_bigarray n in
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

module File = struct
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
    let st = Unix.stat x in
    return (get_vhd_time (st.Unix.st_mtime))

  include Fd
  include Memory
end

module Impl = Vhd.Make(File)
include Impl
include Fd 
