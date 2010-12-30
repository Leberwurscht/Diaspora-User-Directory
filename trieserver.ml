open PTreeDB;;

(***** get the database directory and communication sockets from the command line arguments *****)

let databasedir = Sys.argv.(1);;
let serversockfile = Sys.argv.(2);;
let clientsockfile = Sys.argv.(3);;
let addsockfile = Sys.argv.(4);;
let hashessockfile = Sys.argv.(5);;

(***** prefix tree initialisation *****)

module ZSet = ZZp.Set;;

let timeout = !Settings.reconciliation_config_timeout;;

let settings = { (* copied from reconserver.ml *)
	mbar = !Settings.mbar;
	bitquantum = !Settings.bitquantum;
	treetype = (if !Settings.transactions
		then `transactional
		else if !Settings.disk_ptree 
		then `ondisk else `inmem);
	max_nodes = !Settings.max_ptree_nodes;
	dbdir = databasedir;
	cache_bytes = !Settings.ptree_cache_bytes;
	pagesize = !Settings.ptree_pagesize;
};;

init_db settings;;
init_ptree settings;;

(***** initialize unix domain sockets *****)

(* create recon server socket *)
let () = try Unix.unlink serversockfile with Unix.Unix_error _ -> ();;
let addr = Unix.ADDR_UNIX serversockfile;;
let serversock = Eventloop.create_sock addr;;

(* create recon client socket *)
let () = try Unix.unlink clientsockfile with Unix.Unix_error _ -> ();;
let addr = Unix.ADDR_UNIX clientsockfile;;
let clientsock = Eventloop.create_sock addr;;

(* create add hash socket *)
let () = try Unix.unlink addsockfile with Unix.Unix_error _ -> ();;
let addr = Unix.ADDR_UNIX addsockfile;;
let addsock = Eventloop.create_sock addr;;

(***** helper functions *****)

(* function to iterate over lines of an input stream *)
let rec readline_rec cin buffer = 
	let c = cin#read_char in (* read character *)
	if c='\n' then (
		buffer (* if it's a newline, return the buffer obtained by now *)
	) else (
		Buffer.add_char buffer c; (* otherwise, add the character to the buffer *)
		readline_rec cin buffer (* and continue reading *)
	);;	

let readline cin = 
	let buffer = Buffer.create 32 in (* create empty buffer with size estimation *)
	let buffer = readline_rec cin buffer in (* fill buffer with received characters *)
	let str	= Buffer.sub buffer 0 (Buffer.length buffer) in (* convert buffer to string *)
	str;; (* return string *)

let rec iter_lines cin callback =
	try (
		let line = readline cin in (* read a line *)
			callback line; (* call the callback *)
			iter_lines cin callback (* and continue reading lines *)
	) with End_of_file -> ();; (* except end of file is reached *)

(* function to convert the numbers received from other servers to hashes and send
   them to hashes.ocaml2py.sock *)
let send_number cout number =
	let hash = RMisc.truncate (ZZp.to_bytes number) KeyHash.hash_bytes in
	let hexhash = KeyHash.hexify hash in
	cout#write_string hexhash;
	cout#write_string "\n";
	cout#flush;
	Common.plerror 1 "sent hash %s to hashes.ocaml2py.sock (%s)" hexhash databasedir;;

let send_numbers identifier numbers =
	(* the python part should only accept one connection at a time for this socket *)
	Common.plerror 1 "send_numbers called; connecting to hashes.ocaml2py.sock (%s)" databasedir;
	let socket = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
	let addr = Unix.ADDR_UNIX hashessockfile in
	let cout = Channel.sys_out_from_fd socket in
	Unix.connect socket addr;
	Common.plerror 1 "Connection to hashes.ocaml2py.sock established (%s)" databasedir;
	cout#write_string identifier;
	cout#write_string "\n";
	cout#flush;
	Common.plerror 1 "Sent identifier %s to hashes.ocaml2py.sock (%s)" identifier databasedir;
	ZSet.iter ~f:(send_number cout) numbers;
	Unix.close socket;
	Common.plerror 1 "Connection to hashes.ocaml2py.sock closed (%s)" databasedir;;

(* function to add a hash to the database *)

let add_hash hexhash =
	let l = String.length hexhash in
	if (l=32) then (
		Common.plerror 1 "adding hash %s to database (%s)" hexhash databasedir;
		let binary = KeyHash.dehexify hexhash in
		let modulo = ZZp.of_bytes binary in
		let txn = new_txnopt () in
			PTree.insert (get_ptree ()) txn modulo;
			PTree.clean txn (get_ptree ());
			commit_txnopt txn;
		Common.plerror 1 "added hash %s to database (%s)" hexhash databasedir;
	) else (
		Common.plerror 1 "received line with invalid length %d (%s)" l databasedir;
	)

(***** eventloop callbacks *****)

let servercb addr cin cout =
	Common.plerror 1 "connection on server.ocaml2py.sock (%s)" databasedir;
	let cin = (new Channel.sys_in_channel cin)
	and cout = (new Channel.sys_out_channel cout) in
		let identifier = readline cin in
		let data = Client.handle (get_ptree ()) cin cout in
		send_numbers identifier data;

	Common.plerror 1 "did synchronisation as server as requested by %s (%s)" (ReconMessages.sockaddr_to_string addr) databasedir;
	[];;

let clientcb addr cin cout =
	Common.plerror 1 "connection on client.ocaml2py.sock (%s)" databasedir;
	let cin = (new Channel.sys_in_channel cin)
	and cout = (new Channel.sys_out_channel cout) in
		let identifier = readline cin in
		let data = Server.handle (get_ptree ()) cin cout in
		send_numbers identifier data;

	Common.plerror 1 "did synchronisation as client as requested by %s (%s)" (ReconMessages.sockaddr_to_string addr) databasedir;
	[];;

let addcb addr cin cout =
	Common.plerror 1 "got connection on add.ocaml2py.sock (%s)" databasedir;
	let cin = (new Channel.sys_in_channel cin)
	and cout = (new Channel.sys_out_channel cout) in
		iter_lines cin add_hash;
		ignore(cout);

	[];;

(***** main loop *****)
let run () = Eventloop.evloop [] [
		(serversock, Eventloop.make_th ~name:"serverhandler" ~cb:servercb ~timeout:timeout);
		(clientsock, Eventloop.make_th ~name:"clienthandler" ~cb:clientcb ~timeout:timeout);
		(addsock, Eventloop.make_th ~name:"addhandler" ~cb:addcb ~timeout:timeout)
	];;

(***** run the main loop *****)
Common.protect ~f:run ~finally:(fun () -> 
	closedb (); (* close db to prevent bdb "fatal region errors" *)
	Common.plerror 2 "DB closed (%s)" databasedir
)
