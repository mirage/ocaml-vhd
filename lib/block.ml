(*
 * Copyright (C) Citrix Systems Inc.
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
open Lwt

module IO = Vhd.F.From_file(Vhd_lwt)
open IO

type 'a io = 'a Lwt.t

type id = string

type error = [
  | `Unknown of string
  | `Unimplemented
  | `Is_read_only
  | `Disconnected
]

type page_aligned_buffer = Cstruct.t

type info = {
  read_write: bool;
  sector_size: int;
  size_sectors: int64;
}

type t = {
  mutable vhd: Vhd_lwt.fd Vhd.F.Vhd.t option;
  info: info;
  id: id;
}

let id t = t.id

let connect path =
  Lwt.catch
    (fun () -> Lwt_unix.access path [ Lwt_unix.W_OK ] >>= fun () -> return true)
    (fun _ -> return false)
  >>= fun read_write ->
  Vhd_IO.openchain path read_write >>= fun vhd ->
  let open Vhd.F in
  let sector_size = 512 in
  let size_sectors = Int64.div vhd.Vhd.footer.Footer.current_size 512L in
  let info = { read_write; sector_size; size_sectors } in
  let id = path in
  return (`Ok { vhd = Some vhd; info; id })

let disconnect t = match t.vhd with
  | None -> return ()
  | Some vhd ->
    Vhd_IO.close vhd >>= fun () ->
    t.vhd <- None;
    return ()

let get_info t = return t.info

let to_sectors bufs =
  let rec loop acc remaining =
    if Cstruct.len remaining = 0 then List.rev acc else
    let available = min 512 (Cstruct.len remaining) in
    loop (Cstruct.sub remaining 0 available :: acc) (Cstruct.shift remaining available) in
  List.concat (List.map (loop []) bufs)

let forall_sectors f offset bufs =
  let rec one offset = function
  | [] -> return ()
  | b :: bs -> f offset b >>= fun () -> one (Int64.succ offset) bs in
  one offset (to_sectors bufs)

let zero =
  let buf = Cstruct.create 512 in
  for i = 0 to Cstruct.len buf - 1 do
    Cstruct.set_uint8 buf i 0
  done;
  buf

let read t offset bufs = match t.vhd with
  | None -> return (`Error `Disconnected)
  | Some vhd ->
    forall_sectors
      (fun offset sector ->
        ( Vhd_IO.read_sector vhd offset >>= function
          | None -> return zero
          | Some data -> return data ) >>= fun data ->
        Cstruct.blit data 0 sector 0 512;
        return ()
      ) offset bufs >>= fun () ->
    return (`Ok ())

let write t offset bufs = match t.vhd with
  | None -> return (`Error `Disconnected)
  | Some vhd ->
    forall_sectors (Vhd_IO.write_sector vhd) offset bufs >>= fun () ->
    return (`Ok ())
