def pull_fields(module_name):
    if module_name == "bi_matchkeys":
        velden = pull_fields_matchkeys()
    else:
        velden = pull_fields_normal()

    return velden


def pull_fields_matchkeys():
    velden = ["bi_declnummer", "bi_plaatsingnummer"]

    return velden


def pull_fields_normal():
    #
    # velden = ['bi_typeid',
    #           'bi_type',
    #           'bi_declid',
    #           'bi_declnummer',
    #           'bi_rlregistratienummer',
    #           'bi_plaatsingnummer',
    #           'bi_fwregistratienummer',
    #           'bi_decljaar',
    #           'bi_decldatum',
    #           'bi_declstartdatum',
    #           'bi_decleinddatum',
    #           'bi_looncompnummer',
    #           'bi_kostenplaats',
    #           'bi_fwuren',
    #           'bi_rluren',
    #           'bi_fwuurloon',
    #           'bi_fwpercentage',
    #           'bi_rlpercentage',
    #           'bi_fwaantal',
    #           'bi_rlaantal',
    #           'bi_fwgeldbelast',
    #           'bi_fwgeldonbelast',
    #           'bi_rltarief',
    #           'bi_loonbelast',
    #           'bi_loononbelast',
    #           'bi_verloondatum',
    #           'bi_loonslipnummer',
    #           'bi_loonbelastbuitenstat',
    #           'bi_loononbelastbuitenstat',
    #           'bi_factureerdatum',
    #           'bi_factuurnummer',
    #           'bi_omzet',
    #           'bi_omzetbuitenstat',
    #           'bi_omzethandmatig',
    #           'bi_omzethandmatigbuitenstat',
    #           'bi_status',
    #           'bi_lctype',
    #           'bi_lcnaam',
    #           'bi_sectornaam',
    #           'bi_fwlocatie',
    #           'bi_rllocatie',
    #           'bi_functie']

    velden = []

    return velden
