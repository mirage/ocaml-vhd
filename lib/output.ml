(*
 * Copyright (C) 2013 Citrix Inc
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

(** Stream-orientated vhd access *)

open Vhd

(** A stream consists of a sequence of elements *)
type 'a element =
  | Copy of ('a Vhd.t * int64 * int)
  (** copy a physical block from an underlying file *)
  | Block of Cstruct.t
  (** a new data block (e.g. for metadata) *)
  | Empty of int64
  (** empty space *)

let element_to_string = function
  | Copy(vhd, offset, len) ->
    Printf.sprintf "Copy %s offset = %Ld len = %d" vhd.Vhd.filename offset len
  | Block x ->
    Printf.sprintf "Block len = %d" (Cstruct.len x)
  | Empty x ->
    Printf.sprintf "Empty %Ld" x

module Stream = functor(IO: S.ASYNC) -> struct
  type 'a t =
    | Cons of 'a * (unit -> 'a t IO.t)
    | End

  open IO

  let rec iter f = function
    | Cons(x, rest) ->
      f x >>= fun () ->
      rest () >>= fun x ->
      iter f x
    | End ->
      return ()
end

module Output = functor (File: S.IO) -> struct
  open File
  module S = Stream(File)
  open S

  module V = Make(File)
  open V

  let raw (vhd: File.fd Vhd.t) =
    let block_size_sectors_shift = vhd.Vhd.header.Header.block_size_sectors_shift in
    let max_table_entries = Int32.to_int vhd.Vhd.header.Header.max_table_entries in
    let empty_block = Empty (Int64.shift_left 1L (block_size_sectors_shift + sector_shift)) in
    let empty_sector = Empty (Int64.shift_left 1L sector_shift) in
    let rec block i =
      let next_block () = block (i + 1) in
      if i = max_table_entries
      then return End
      else begin
        if vhd.Vhd.bat.(i) = BAT.unused
        then return (Cons(empty_block, next_block))
        else begin
          let rec sector j =
            let next_sector () = sector (j + 1) in
            if j = 1 lsl block_size_sectors_shift
            then next_block ()
            else begin
              let absolute_sector = Int64.(shift_left (of_int i) (block_size_sectors_shift + j)) in
              Vhd_IO.get_sector_location vhd absolute_sector >>= function
              | None ->
                return (Cons(empty_sector, next_sector))
              | Some (vhd', offset) ->
                return (Cons(Copy(vhd', offset, sector_size), next_sector))
            end in
          sector 0
        end
      end in
    block 0
end
