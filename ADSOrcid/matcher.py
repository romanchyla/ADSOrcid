
def find_orcid_position(orcid, list_of_names):
    """
    Find the position of ORCID in the list of other strings
    
    :param orcid_name - String
    :param list_of_names - array of strings
    :return list of positions that match
    """
    orcid_data = retrieve_orcid(orcid)
    pass
    
#TODO: add memoize decorator    
def retrieve_orcid(orcid):
    """
    Finds (or creates and returns) model of ORCID
    from the dbase
    
    :param orcid - String (orcid id)
    :return - OrcidModel datastructure
    """
    pass    