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

val verify: Vhd_format_lwt.IO.fd Vhd_format.F.Vhd.t -> (int64 * Cstruct.t) list -> unit Lwt.t
(** [verify vhd sectors] performs various checks on [vhd] to ensure it has
    exactly the content given by [sectors], an association list of sector
    number to 512-byte block. *)

val header : Vhd_format.F.Header.t Alcotest.testable

val footer : Vhd_format.F.Footer.t Alcotest.testable

val bat : Vhd_format.F.BAT.t Alcotest.testable

val cstruct : Cstruct.t Alcotest.testable
