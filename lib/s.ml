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
  val return: 'a -> 'a t
end

module type File = sig
  include ASYNC

  type fd

  val exists: string -> bool t
  val openfile: string -> fd t
  val really_read: fd -> int -> int -> Cstruct.t t
  val really_write: fd -> int -> Cstruct.t -> unit t
end


