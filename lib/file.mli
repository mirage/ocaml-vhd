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

val use_unbuffered: bool ref
(** if set to true we will use unbuffered I/O via O_DIRECT *)

val openfile: string -> int -> Unix.file_descr
(** [openfile filename mode] opens [filename] read/write using
    the current global buffering mode *)

val get_file_size: string -> int64
(** [fet_file_size filename] returns the number of bytes in
    [filename] *)

val fsync: Unix.file_descr -> unit
(** [fsync fd] ensures that any buffered data is written to disk
    and throws a Unix_error if any error has been recorded. *)
