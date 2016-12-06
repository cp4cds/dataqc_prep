# import statements
#import dateutil.parser as dt
import collections, os, timeit
import cProfile
import time
import requests
import itertools

def check_availability():
    """
    Main program
    Run check of data availability for all monthly variables
    :return: output to file
    """
    # SET CONSTRAINTS
    all_vars = define_all_variables()
    core_vars = ['tas', 'tasmax', 'tasmin', 'ps', 'uas', 'vas', 'pr']
    tri_core_expts=['historical', 'piControl', 'rcp45']
    all_core_expts=['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']

    node = "esgf-index1.ceda.ac.uk"
    project = "CMIP5"
    latest = 'True'
    distrib = 'False'

    # Loop over two different numbers of ensembles
    for nens in [0]:
        # Loop over two different sets of experiments
        for expts in [tri_core_expts, all_core_expts]:

            filename = filename_constructor(nens, expts)

            # open file to store output
            with open(filename, "w") as models_writer:

                for var, tables in all_vars.items():
                    for table in tables:
                        # For each variable in a given cmor_table
                        # Get the CMIP5 models that have the required number of ensemble members
                        models_by_expt = get_models(node, project, var, table, expts, latest, distrib)
                        # Get all the models that exist in all the experiments
                        valid_models = check_in_all_models(models_by_expt)

                        # Calculate this volume of data; write to cache/
                        total_volume = get_data_volume(node, project, var, table, expts, latest, distrib, valid_models)

                        # Write aggregates to file
                        #models_writer.write("%s, %s, %s, %s\n" % (var, table, len(valid_models), total_volume))
                        print var, table, len(valid_models), total_volume
                        models_writer.write("%s, %s, %s, %s \n" % (var, table, len(valid_models), total_volume))


def get_data_volume(node, project, var, table, exptsList, latest, distrib, valid_models):
    """
    Get data volumes using the ESGF Search RESTful API

    :param node: node at which to test
    :param project: search project
    :param var: variable of interest
    :param table: cmor_table associated with variable
    :param expts: list of experiments
    :param latest: latest True/False search
    :param distrib: distributed search
    :param valid_models: set of valid models

    :return: total size for search in GB
    """

    # Format models and experiments a comma separated lists so url is well formatted
    models = ', '.join(map(str, valid_models))
    expts = ', '.join(map(str, exptsList))

    url = "https://%(node)s/esg-search/search?type=File" \
          "&project=%(project)s&experiment=%(expts)s&variable=%(var)s&cmor_table=%(table)s" \
          "&latest=%(latest)s&distrib=%(distrib)s&model=%(models)s&format=application%%2Fsolr%%2Bjson&limit=10000" \
          % vars()

    resp = requests.get(url)
    json = resp.json()

    # Perform size calculation
    records = json["response"]["docs"]
    size = 0
    for i in range(len(records)):
        size += records[i]["size"]


    return size / (1024.**3)


def get_models(node, project, var, table, expts, latest, distrib):
    """
    Get a list of all models for a given variable for each experiment and
    store them in a dictionary

    :param node: node at which to test
    :param project: search project
    :param var: variable of interest
    :param table: cmor_table associated with variable
    :param expts: list of experiments
    :param latest: latest True/False search
    :param distrib: distributed search

    :return: Dictionary of models satisfying the above search criteria
    """

    result = {}
    for expt in expts:

        url = "https://%(node)s/esg-search/search?type=File" \
              "&project=%(project)s&experiment=%(expt)s&variable=%(var)s&cmor_table=%(table)s" \
              "&latest=%(latest)s&distrib=%(distrib)s&facets=model&format=application%%2Fsolr%%2Bjson&limit=1000" \
              % vars()

        resp = requests.get(url)
        json = resp.json()

        mods = json["facet_counts"]["facet_fields"]['model']
        models = dict(itertools.izip_longest(*[iter(mods)] * 2, fillvalue=""))

        """
        TODO check if models have a specific number of ensemble members
        or generate a list of ensemble members per model?
        """
        result[expt] = models.keys()

    return result


def check_in_all_models(models_per_expt):
    """
    Check intersection of which models are in all experiments

    :param models_per_expt: an ordered dictionary of expts as keys with a list of valid models
    :return: list of models that appear in all experiments
    """

    in_all = None

    for key, items in models_per_expt.items():
        if in_all is None:
            in_all = set(items)
        else:
            in_all.intersection_update(items)
    return in_all


def filename_constructor(nens, expts):
    """
    Dynamic filename constructor

    :param nens:    Number of minimum ensemble members
    :param expts:   Number of experiments

    :return:        filename
    """

    if expts == ['historical', 'piControl', 'rcp45']:
        file_expt_string = 'tri_core_expts_models_avail_'
    if expts == ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']:
        file_expt_string = 'all_core_expts_models_avail_'
    if nens == 0:
        file_ens_string = 'any_ensembles_url.txt'
    if nens == 2:
        file_ens_string = 'three_ensembles_url.txt'

    # construct filename
    return 'output/' + file_expt_string + file_ens_string


def define_all_variables():
    """
    A routine to declare the variables of interest
    in an ordered dictionary of the form
    variable, table

    :return: an ordered dictionary of all variables
    """
    """
    A copy of all vars as normal dictionary for reference
    all_vars_dict = {'tas': ['Amon', 'day'], 'ts': ['Amon'], 'tasmax': ['Amon', 'day'], 'tasmin': ['Amon', 'day'],
                'psl': ['Amon', 'day'], 'ps': ['Amon'], 'uas': ['Amon'], 'vas': ['Amon'], 'sfcWind': ['Amon', 'day'],
                'hurs': ['Amon'], 'huss': ['Amon', 'day'], 'pr': ['Amon', 'day'], 'prsn': ['Amon'], 'evspsbl': ['Amon'],
                'tauu': ['Amon'], 'tauv': ['Amon'], 'hfls': ['Amon'], 'hfss': ['Amon'], 'rlds': ['Amon'], 'rlus': ['Amon'],
                'rsds': ['Amon'], 'rsus': ['Amon'], 'rsdt': ['Amon'], 'rsut': ['Amon'], 'rlut': ['Amon'], 'clt': ['Amon'],
                'mrsos': ['Lmon'], 'mrro': ['Lmon'], 'snw': ['LImon'], 'tos': ['Omon'], 'sos': ['Omon'], 'zos': ['Omon'],
                'sic': ['OImon'], 'sit': ['OImon'], 'snd': ['OImon'], 'sim': ['OImon'], 'tsice': ['OImon'], 'ta': ['Amon'],
                'ua': ['Amon'], 'va': ['Amon'], 'hur': ['Amon'], 'hus': ['Amon'], 'zg': ['Amon']}
    """

    vars = collections.OrderedDict()
    vars['tas'] = ['Amon', 'day']
    vars['ts'] = ['Amon']
    vars['tasmax'] = ['Amon', 'day']
    vars['tasmin'] = ['Amon', 'day']
    vars['psl'] = ['Amon', 'day']
    vars['ps'] = ['Amon']
    vars['uas'] = ['Amon']
    vars['vas'] = ['Amon']
    vars['sfcWind'] = ['Amon', 'day']
    vars['hurs'] = ['Amon']
    vars['huss'] = ['Amon', 'day']
    vars['pr'] = ['Amon', 'day']
    vars['prsn'] = ['Amon']
    vars['evspsbl'] = ['Amon']
    vars['tauu'] = ['Amon']
    vars['tauv'] = ['Amon']
    vars['hfls'] = ['Amon']
    vars['hfss'] = ['Amon']
    vars['rlds'] = ['Amon']
    vars['rlus'] = ['Amon']
    vars['rsds'] = ['Amon']
    vars['rsus'] = ['Amon']
    vars['rsdt'] = ['Amon']
    vars['rsut'] = ['Amon']
    vars['rlut'] = ['Amon']
    vars['clt'] = ['Amon']
    vars['mrsos'] = ['Lmon']
    vars['mrro'] = ['Lmon']
    vars['snw'] = ['LImon']
    vars['tos'] = ['Omon']
    vars['sos'] = ['Omon']
    vars['zos'] = ['Omon']
    vars['sic'] = ['OImon']
    vars['sit'] = ['OImon']
    vars['snd'] = ['OImon']
    vars['sim'] = ['OImon']
    vars['tsice'] = ['OImon']
    vars['ta'] = ['Amon']
    vars['ua'] = ['Amon']
    vars['va'] = ['Amon']
    vars['hur'] = ['Amon']
    vars['hus'] = ['Amon']
    vars['zg'] = ['Amon']

    return vars


if __name__ == '__main__':
    check_availability()
