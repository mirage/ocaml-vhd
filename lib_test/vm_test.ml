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
open OUnit
open Lwt

module Impl = Vhd.Make(Vhd_lwt)
open Impl
open Vhd
open Vhd_lwt
open Patterns
open Patterns_lwt


let fill_sector_with pattern =
  let b = Memory.alloc 512 in
  for i = 0 to 511 do
    Cstruct.set_char b i (pattern.[i mod (String.length pattern)])
  done;
  b

let twomib = Int64.(mul (mul 2L 1024L) 1024L)

let get_virtual_size vdi =
  Printf.fprintf stderr "VDI.get_virtual_size %s\n%!" vdi;
  return 0L

let absolute_sector_of vdi { block; sector } =
  (* assume 2 MiB block size *)
  get_virtual_size vdi >>= fun virtual_size ->
  if virtual_size < twomib
  then return None
  else
    let block = match block with
    | First -> 0L
    | Last -> Int64.div virtual_size twomib in
    let sector = match sector with
    | First -> 0L
    | Last -> Int64.sub twomib 512L in
    return (Some(Int64.add block sector))

let cstruct_to_string c = String.escaped (Cstruct.to_string c)

type state = {
  to_destroy: string list;
  child: string option;
  contents: (int64 * Cstruct.t) list;
}

let initial = {
  to_destroy = [];
  child = None;
  contents = [];
}

let sectors = Hashtbl.create 16
let sector_lookup message =
  if Hashtbl.mem sectors message
  then Hashtbl.find sectors message
  else
    let data = fill_sector_with message in
    Hashtbl.replace sectors message data;
    data

let vdi_create size =
  Printf.fprintf stderr "VDI.create %Ld\n%!" size;
  return ""

let vdi_snapshot vdi =
  Printf.fprintf stderr "VDI.snapshot %s\n%!" vdi;
  return ""

let vdi_write vdi offset data =
  Printf.fprintf stderr "write %s @%Ld\n%!" vdi offset;
  return ()

let vdi_destroy vdi =
  Printf.fprintf stderr "VDI.destroy %s\n%!" vdi;
  return ()

let execute state = function
  | Create size ->
    vdi_create size >>= fun vdi ->
    return {
      to_destroy = vdi :: state.to_destroy;
      child = Some vdi;
      contents = [];
    }
  | Snapshot ->
    let vdi = match state.child with
    | Some vdi -> vdi
    | None -> failwith "no vdi" in
    vdi_snapshot vdi >>= fun snap ->
    return {
      to_destroy = snap :: state.to_destroy;
      child = state.child;
      contents = state.contents;
    }
  | Write (position, message) ->
    let data = sector_lookup message in
    let vdi = match state.child with
    | Some vdi -> vdi
    | None -> failwith "no vdi open" in
    absolute_sector_of vdi position >>= fun offset ->
    begin match offset with
      | Some sector ->
        vdi_write vdi sector data >>= fun () ->
        (* Overwrite means we forget any previous contents *)
        let contents = List.filter (fun (x, _) -> x <> sector) state.contents in
        return { state with contents = (sector, data) :: contents }
      | None ->
        return state
    end

let open_vdi_locally t = failwith "open_vdi_locally"

let verify state = match state.child with
  | None -> return ()
  | Some t -> verify (open_vdi_locally t) state.contents

let cleanup state =
  Lwt_list.iter_s vdi_destroy state.to_destroy

let run program =
  let single_instruction state x =
    execute state x >>= fun state' ->
    verify state' >>= fun () ->
    return state' in
  Lwt_list.fold_left_s single_instruction initial program >>= fun final_state ->
  cleanup final_state

let all_program_tests = List.map (fun p ->
  (string_of_program p) >:: (fun () -> Lwt_main.run (run p))
) programs

let _ =
  let verbose = ref false in
  Arg.parse [
    "-verbose", Arg.Unit (fun _ -> verbose := true), "Run in verbose mode";
  ] (fun x -> Printf.fprintf stderr "Ignoring argument: %s" x)
    "Test vhd parser";

  let suite = "vhd" >::: all_program_tests in
  run_test_tt ~verbose:!verbose suite

