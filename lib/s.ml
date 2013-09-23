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

module type ASYNC = sig
  type 'a t

  val (>>=): 'a t -> ('a -> 'b t) -> 'b t
  val fail: exn -> 'a t
  val return: 'a -> 'a t
end

module type TIME = sig
  val now: unit -> int32
end

module type MEMORY = sig
  val alloc: int -> Cstruct.t
end

module type RW = sig
  include ASYNC

  type handle 

  val really_read: handle -> int64 -> int -> Cstruct.t t
  val really_write: handle -> int64 -> Cstruct.t -> unit t
  val really_read_into: handle -> int64 -> Cstruct.t -> Cstruct.t t
end

module type IO = sig
  include TIME
  include MEMORY
  type fd
  include RW with type handle := fd

  val exists: string -> bool t
  val openfile: string -> fd t
  val fsync: fd -> unit
  val create: string -> fd t
  val close: fd -> unit t
  val get_file_size: string -> int64 t
  val get_modification_time: string -> int32 t

end

