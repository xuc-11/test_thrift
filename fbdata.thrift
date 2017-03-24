struct Triple {
1: string s,
2: string p,
3: string o,
}

service FBDataService {
    map<string,list<string>> get_1hop_dict_by_mid(1:string mid)
    list<Triple> get_1hop_triple_by_mid(1:string mid)
    list<list<Triple>> get_1or2hop_triple_by_mid(1:string mid)
    list<list<Triple>> get_paths_from_2mid(1:string mid1,2:string mid2)
}