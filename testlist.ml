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

let tree = get_ptree ();;

let root = PrefixTree.root tree;;

print_string "\n";;
print_string "Elements (as numbers):\n";;

let elements = PrefixTree.elements tree root;;

ZSet.iter ~f:(fun s -> Printf.printf "%s\n" (Number.to_string (ZZp.to_number s))) elements;;

print_string "\n";;
Printf.printf "Elements (as hashes in hexadecimal representation):\n";;

ZSet.iter ~f:(fun s -> Printf.printf "%s\n" (KeyHash.hexify (ZZp.to_bytes s))) elements;;
