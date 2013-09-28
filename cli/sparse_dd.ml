(* Utility program which copies between two block devices, using vhd BATs and efficient zero-scanning
   for performance. *)

let config_file = "/etc/sparse_dd.conf"

type encryption_mode =
  | Always
  | Never
  | User
let string_of_encryption_mode = function
  | Always -> "always"
  | Never -> "never"
  | User -> "user"
let encryption_mode_of_string = function
  | "always" -> Always
  | "never" -> Never
  | "user" -> User
  | x -> failwith (Printf.sprintf "Unknown encryption mode %s. Use always, never or user." x)
let encryption_mode = ref Never

let base = ref None 
let src = ref None
let dest = ref None
let size = ref (-1L)
let prezeroed = ref false
let set_machine_logging = ref false
let experimental_writes_bypass_tapdisk = ref false

let string_opt = function
  | None -> "None"
  | Some x -> x

let options = [
    "unbuffered", Arg.Bool (fun b -> File.use_unbuffered := b), (fun () -> string_of_bool !File.use_unbuffered), "use unbuffered I/O via O_DIRECT";
    "encryption-mode", Arg.String (fun x -> encryption_mode := encryption_mode_of_string x), (fun () -> string_of_encryption_mode !encryption_mode), "how to use encryption";
    "experimental-writes-bypass-tapdisk", Arg.Set experimental_writes_bypass_tapdisk, (fun () -> string_of_bool !experimental_writes_bypass_tapdisk), "bypass tapdisk and write directly to the underlying vhd file";
    "base", Arg.String (fun x -> base := Some x), (fun () -> string_opt !base), "base disk to search for differences from";
    "src", Arg.String (fun x -> src := Some x), (fun () -> string_opt !src), "source disk";
    "dest", Arg.String (fun x -> dest := Some x), (fun () -> string_opt !dest), "destination disk";
    "size", Arg.String (fun x -> size := Int64.of_string x), (fun () -> Int64.to_string !size), "number of bytes to copy";
    "prezeroed", Arg.Set prezeroed, (fun () -> string_of_bool !prezeroed), "assume the destination disk has been prezeroed";
    "machine", Arg.Set set_machine_logging, (fun () -> string_of_bool !set_machine_logging), "emit machine-readable output";
]

open Xenstore

let ( +* ) = Int64.add
let ( -* ) = Int64.sub
let ( ** ) = Int64.mul
let kib = 1024L
let mib = kib ** kib

let (|>) a b = b a
module Opt = struct
	let default d = function
		| None -> d
		| Some x -> x
end
module Mutex = struct
	include Mutex
	let execute m f =
		Mutex.lock m;
		try
			let result = f () in
			Mutex.unlock m;
			result
		with e ->
			Mutex.unlock m;
			raise e
end

type logging_mode =
	| Buffer (* before we know which output format we should use *)
	| Human
	| Machine

let logging_mode = ref Buffer

let buffer = ref []

let log_m = Mutex.create ()

let log (fmt: ('a , unit, string, unit) format4) =
	let header = Cstruct.create Chunked.sizeof in
	Mutex.execute log_m
		(fun () ->
			Printf.kprintf
				(fun s ->
					match !logging_mode with
						| Buffer ->
							buffer := s :: !buffer
						| Human ->
							Printf.printf "%s\n%!" s
						| Machine ->
							let data = Cstruct.create (String.length s) in
							Cstruct.blit_from_string s 0 data 0 (String.length s);
							Chunked.marshal header { Chunked.offset = 0L; data };
							Printf.printf "%s%s%!" (Cstruct.to_string header) s
				) fmt
		)
let debug (fmt: ('a , unit, string, unit) format4) = log fmt
let warn  (fmt: ('a , unit, string, unit) format4) = log fmt


let set_logging_mode m =
	let to_flush = Mutex.execute log_m
		(fun () ->
			logging_mode := m;
			match m with
				| Human
				| Machine ->
					List.rev !buffer
				| Buffer -> []
		) in
	List.iter (fun x -> debug "%s" x) to_flush

let close_output () =
	Mutex.execute log_m
		(fun () ->
			match !logging_mode with
				| Machine ->
					let header = Cstruct.create Chunked.sizeof in
					Chunked.marshal header { Chunked.offset = 0L; data = Cstruct.create 0 };
					Printf.printf "%s%!" (Cstruct.to_string header)
				| _ -> ()
		)

let startswith prefix x =
	let prefix' = String.length prefix
	and x' = String.length x in
	prefix' <= x' && (String.sub x 0 prefix' = prefix)

(** [find_backend_device path] returns [Some path'] where [path'] is the backend path in
    the driver domain corresponding to the frontend device [path] in this domain. *)
let find_backend_device path =
	try 
		let open Xenstore in
		(* If we're looking at a xen frontend device, see if the backend
		   is in the same domain. If so check if it looks like a .vhd *)
		let rdev = (Unix.stat path).Unix.st_rdev in
		let major = rdev / 256 and minor = rdev mod 256 in
		let link = Unix.readlink (Printf.sprintf "/sys/dev/block/%d:%d/device" major minor) in
		match List.rev (Re_str.split (Re_str.regexp_string "/") link) with
		| id :: "xen" :: "devices" :: _ when startswith "vbd-" id ->
			let id = int_of_string (String.sub id 4 (String.length id - 4)) in
			with_xs (fun xs -> 
				let self = xs.Xs.read "domid" in
				let backend = xs.Xs.read (Printf.sprintf "device/vbd/%d/backend" id) in
				let params = xs.Xs.read (Printf.sprintf "%s/params" backend) in
				match Re_str.split (Re_str.regexp_string "/") backend with
				| "local" :: "domain" :: bedomid :: _ ->
					assert (self = bedomid);
					Some params
				| _ -> raise Not_found
			)
		| _ -> raise Not_found
	with _ -> None
(** [vhd_of_device path] returns (Some vhd) where 'vhd' is the vhd leaf backing a particular device [path] or None.
    [path] may either be a blktap2 device *or* a blkfront device backed by a blktap2 device. If the latter then
    the script must be run in the same domain as blkback. *)
let vhd_of_device path =
	let tapdisk_of_path path =
		try 
			match Tapctl.of_device (Tapctl.create ()) path with
			| _, _, (Some (_, vhd)) -> Some vhd
			| _, _, _ -> raise Not_found
		with Tapctl.Not_blktap ->
			debug "Device %s is not controlled by blktap" path;
			None
		| Tapctl.Not_a_device ->
			debug "%s is not a device" path;
			None
		| _ -> 
			debug "Device %s has an unknown driver" path;
			None in
	find_backend_device path |> Opt.default path |> tapdisk_of_path

let after f g =
	try
		let r = f () in
		g ();
		r
	with e ->
		g ();
		raise e

let with_paused_tapdisk path f =
	let path = find_backend_device path |> Opt.default path in

	let context = Tapctl.create () in
	match Tapctl.of_device context path with
	| tapdev, _, (Some (driver, path)) ->
		debug "pausing tapdisk for %s" path;
		Tapctl.pause context tapdev;
		after f (fun () ->
			debug "unpausing tapdisk for %s" path;
			Tapctl.unpause context tapdev path Tapctl.Vhd
		)
	| _, _, _ -> failwith (Printf.sprintf "Failed to pause tapdisk for %s" path)

let deref_symlinks path = 
	let rec inner seen_already path = 
		if List.mem path seen_already
		then failwith "Circular symlink";
		let stats = Unix.LargeFile.lstat path in
		if stats.Unix.LargeFile.st_kind = Unix.S_LNK
		then inner (path :: seen_already) (Unix.readlink path)
		else path in
	inner [] path


(* Record when the binary started for performance measuring *)
let start = Unix.gettimeofday ()

(* Helper function to print nice progress info *)
let progress_cb =
	let last_percent = ref (-1) in

	function fraction ->
		let new_percent = int_of_float (fraction *. 100.) in
		if !last_percent <> new_percent then begin
			if !logging_mode = Machine
			then debug "Progress: %.0f" (fraction *. 100.)
			else debug "\b\rProgress: %-60s (%d%%)" (String.make (int_of_float (fraction *. 60.)) '#') new_percent;
			flush stdout;
		end;
		last_percent := new_percent

let _ =
	File.use_unbuffered := true;
	Xcp_service.configure ~options ();
	if !set_machine_logging then set_logging_mode Machine;
	if !logging_mode = Buffer then set_logging_mode Human;

	let src = match !src with
		| None ->
			debug "Must have -src argument\n";
			exit 1
		| Some x -> x in
	let dest = match !dest with
		| None ->
			debug "Must have -dest argument\n";
			exit 1
		| Some x -> x in
	if !size = (-1L) then begin
		debug "Must have -size argument\n";
		exit 1
	end;
	let size = !size in
	let base = !base in

	debug "src = %s; dest = %s; base = %s; size = %Ld" src dest (Opt.default "None" base) size;
	let src_vhd = vhd_of_device src in
	let dest_vhd = vhd_of_device dest in
	let base_vhd = match base with
		| None -> None
		| Some x -> vhd_of_device x in
	debug "src_vhd = %s; dest_vhd = %s; base_vhd = %s" (Opt.default "None" src_vhd) (Opt.default "None" dest_vhd) (Opt.default "None" base_vhd);

	let source, source_format = match src, src_vhd with
	| _, Some vhd -> vhd, "vhd"
	| device, None -> device, "raw" in
	let destination, destination_format = match !experimental_writes_bypass_tapdisk, dest, dest_vhd with
	| true, _, Some vhd ->
		warn "experimental_writes_bypass_tapdisk set: this may cause data corruption";
		prezeroed := false; (* the physical disk will have vhd metadata and other stuff on it *)
		"file://" ^ vhd, "vhd"
        | _, device_or_url, _ ->
		let uri = Uri.of_string device_or_url in
		let rewrite_scheme scheme =
			let uri = Uri.make ~scheme
				?userinfo:(Uri.userinfo uri)
				?host:(Uri.host uri)
				?port:(Uri.port uri)
				~path:(Uri.path uri)
				~query:(Uri.query uri)
				?fragment:(Uri.fragment uri)
				() in
			Uri.to_string uri in
		begin match Uri.scheme uri with
		| Some "https" when !encryption_mode = Never ->
			warn "turning off encryption for this transfer as requested by config file";
			rewrite_scheme "http", "raw"
		| Some "http" when !encryption_mode = Always ->
			warn "turning on encryption for this transfer as requested by config file";
			rewrite_scheme "https", "raw"
		| _ -> "file://" ^ device_or_url, "raw"
		end in
	let relative_to = base_vhd in

	let common = Common.make false false true in

	progress_cb 0.;
        let progress total_work work_done =
          let fraction = Int64.(to_float work_done /. (to_float total_work)) in
          progress_cb fraction in
        let t = Impl.stream_t common source relative_to source_format destination_format destination (Some "none") None !prezeroed ~progress () in
	if destination_format = "vhd"
	then with_paused_tapdisk dest (fun () -> Lwt_main.run t)
	else Lwt_main.run t;
	let time = Unix.gettimeofday () -. start in
	debug "Time: %.2f seconds" time;
	close_output ()
