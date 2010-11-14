open PTreeDB;;

module ZSet = ZZp.Set;;

let settings = { (* copied from reconserver.ml *)
	mbar = !Settings.mbar;
	bitquantum = !Settings.bitquantum;
	treetype = (if !Settings.transactions
		then `transactional
		else if !Settings.disk_ptree 
		then `ondisk else `inmem);
	max_nodes = !Settings.max_ptree_nodes;
	dbdir = Lazy.force Settings.ptree_dbdir;
	cache_bytes = !Settings.ptree_cache_bytes;
	pagesize = !Settings.ptree_pagesize;
};;

init_db settings;;
init_ptree settings;;

let add_number number =
	Printf.printf "got %s\n" (Number.to_string (ZZp.to_number number));
	let txn = new_txnopt () in
		PTree.insert (get_ptree ()) txn number;
		PTree.clean txn (get_ptree ());
		commit_txnopt txn

(* create recon server socket *)
let unixsockaddr = "server.ocaml2py.sock";;
let () = try Unix.unlink unixsockaddr with Unix.Unix_error _ -> ();;
let addr = Unix.ADDR_UNIX unixsockaddr;;
let serversock = Eventloop.create_sock addr;;

(* create recon client socket *)
let unixsockaddr = "client.ocaml2py.sock";;
let () = try Unix.unlink unixsockaddr with Unix.Unix_error _ -> ();;
let addr = Unix.ADDR_UNIX unixsockaddr;;
let clientsock = Eventloop.create_sock addr;;

(* create add hash socket *)
let unixsockaddr = "add.ocaml2py.sock";;
let () = try Unix.unlink unixsockaddr with Unix.Unix_error _ -> ();;
let addr = Unix.ADDR_UNIX unixsockaddr;;
let addsock = Eventloop.create_sock addr;;

(* socket to transmit the hashes received from other servers to python *)
let othersockaddr = "hashes.ocaml2py.sock";;

let timeout = !Settings.reconciliation_config_timeout;;

let send_number cout number =
	cout#write_string (ZZp.to_bytes number);
	cout#write_string "\n";
	cout#flush;;

let send_numbers numbers =
	(* the python part should only accept one connection at a time for this socket *)
	let socket = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
	let addr = Unix.ADDR_UNIX othersockaddr in
	let cout = Channel.sys_out_from_fd socket in
	Unix.connect socket addr;
	ZSet.iter ~f:(send_number cout) numbers;
	Unix.close socket;;
(*	ZSet.iter ~f:(fun number -> send_number cout number) numbers;; *)

let testserver addr cin cout =
	let cin = (new Channel.sys_in_channel cin)
	and cout = (new Channel.sys_out_channel cout) in
		let data = Client.handle (get_ptree ()) cin cout in
(*		ZSet.iter ~f:add_number data; *)
		send_numbers data;

	Common.plerror 1 "talked with %s" (ReconMessages.sockaddr_to_string addr);
	[];;

let testclient addr cin cout =
	let cin = (new Channel.sys_in_channel cin)
	and cout = (new Channel.sys_out_channel cout) in
		let data = Server.handle (get_ptree ()) cin cout in
(*		ZSet.iter ~f:add_number data; *)
		send_numbers data;

	Common.plerror 1 "did synchronisation as client as requested by %s" (ReconMessages.sockaddr_to_string addr);
	[];;

(*

	Number <-> Hash conversion
	==========================
	Number -> ZZp.to_bytes -> hexadecimal representation
	

*)

(*let rec read_line_rec cin cout len = 
	let c = cin#read_char in
	if c='\n'
	then (
		cout#write_string "got line.";
		cout#flush
	) else (
		cout#write_string "got char.";
		cout#flush;
		read_line cin cout
	);;

let read_line cin cout len = 
	let s = String.create len in
*)	



let testadd addr cin cout =
	let cin = (new Channel.sys_in_channel cin)
	and cout = (new Channel.sys_out_channel cout) in
		let str = cin#read_string 16 in
		let t = cin#read_char in
		let n = ZZp.of_bytes str in
		ignore(t);
		let txn = new_txnopt () in
			PTree.insert (get_ptree ()) txn n;
			PTree.clean txn (get_ptree ());
			commit_txnopt txn;
(*		cout#write_string "Added number ";
		cout#write_string (Number.to_string (ZZp.to_number n));
		cout#write_string "\n";
(*		read_line cin cout;*)
		(* read 16 bytes each *)
(*		let hash = cin#read_string 33 in
		cout#write_string "got\n";
		cout#write_string hash;
(* need to convert hash string to a zset number -- zzp from bytes, to bytes, see recoverlist *)
(* other way round: hashconvert, see reconserver:189 *)
		cout#write_string "test\n"; *)
		cout#flush;*)

(*	Common.plerror 1 "did synchronisation as client as requested by %s" (ReconMessages.sockaddr_to_string addr);*)
	[];;

(*Common.plerror 1 "" RecoverList;; *)
(*
let tree = get_ptree ();;

let root = PrefixTree.root tree;;

let elements = PrefixTree.elements tree root;;
let ele = ZSet.elements elements;;

let hashes = RecoverList.hashconvert ele;; 

RecoverList.print_hashes "bla" hashes;; *)

Eventloop.evloop [] [
	(serversock, Eventloop.make_th ~name:"testserver" ~cb:testserver ~timeout:timeout);
	(clientsock, Eventloop.make_th ~name:"testclient" ~cb:testclient ~timeout:timeout);
	(addsock, Eventloop.make_th ~name:"testadd" ~cb:testadd ~timeout:timeout)
];;
