# import statements
#import dateutil.parser as dt
import collections, os, timeit
import cProfile
import time
import requests
import itertools

def get_info():
    """
    Some code to pull back some basic file info:
        filename, filesize, forcings, checksum_type, checksum, tracking_id, replica

    :return: Write to txt file:
        /group_workspaces/jasmin/cp4cds1/data_availability/cache/
    """


    mon_vars, day_vars = define_all_variables()
    all_core_expts = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']

    node = "esgf-index1.ceda.ac.uk"
    project = "CMIP5"
    latest = 'True'
    distrib = 'False'

    # Loop over two different sets of experiments
    for expt in all_core_expts:

        for var, tables in mon_vars.items():
            for table in tables:
                get_var_info(node, project, var, table, expt, latest, distrib)

        for var, tables in day_vars.items():
            for table in tables:
                get_var_info(node, project, var, table, expt, latest, distrib)


def get_var_info(node, project, var, table, expt, latest, distrib):
    """
    Get models and ensemble members for given variable, table and experiment
    Write output to file.

    :param node:
    :param project:
    :param var:
    :param table:
    :param expt:
    :param latest:
    :param distrib:
    :return: Output to file
    """

    # Get list of models
    valid_models = get_models(node, project, var, table, expt, latest, distrib)
    # Get list of ensembles
    for model in valid_models:
        valid_ensembles = get_ensembles(node, project, var, table, expt, model, latest, distrib)

        for ensemble in valid_ensembles:
            get_file_info(node, project, var, table, expt, model, ensemble, latest, distrib)


def get_file_info(node, project, var, table, expt, model, ensemble, latest, distrib):
    """
    Get a list of all models for a given variable for each experiment and
    store them in a dictionary

    :param node: node at which to test
    :param project: search project
    :param var: variable of interest
    :param table: cmor_table associated with variable
    :param expt: experiment
    :param model: model
    :param ensemble: ensemble member
    :param latest: latest True/False search
    :param distrib: distributed search

    :return: List of models satisfying the above search criteria
    """

    cache_dir = '/group_workspaces/jasmin/cp4cds1/data_availability/cache'
    if not os.path.isdir(cache_dir):
        os.mkdir(cache_dir)

    filename = '_'.join([var, table, expt, model, ensemble]) + '.txt'
    cache_file = os.path.join(cache_dir, filename)

    if not os.path.isfile(cache_file):

        url = "https://%(node)s/esg-search/search?type=File" \
              "&project=%(project)s&experiment=%(expt)s&variable=%(var)s&cmor_table=%(table)s" \
              "&latest=%(latest)s&distrib=%(distrib)s&model=%(model)s&ensemble=%(ensemble)s&format=application%%2Fsolr%%2Bjson&limit=1000" \
              % vars()

        resp = requests.get(url)
        json = resp.json()

        records = json["response"]["docs"]

        with open(cache_file, "w") as cache_writer:

            for record in range(len(records)):

                filename = records[record]['url'][0]
                filename = filename.replace("http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot", "/badc/cmip5/data")
                filename = filename.split('|')[0]
                dataset_id = records[record]['dataset_id']
                dataset_id = dataset_id.rsplit('|')[0]
                version =  dataset_id.rsplit('.')[-1]
                download_url = records[record]['url'][2]
                variable = records[record]['variable'][0]
                variable_cf_name = records[record]['cf_standard_name'][0]
                variable_long_name = records[record]['variable_long_name'][0]
                variable_units = records[record]['variable_units'][0]
                experiment_family = records[record]['experiment_family']
                product = records[record]['product'][0]
                filesize = records[record]['size']
                forcings = records[record]['forcing']
                checksum_type = records[record]['checksum_type'][0]
                checksum = records[record]['checksum'][0]
                tracking_id = records[record]['tracking_id'][0]
                index_node = records[record]['index_node']
                replica = records[record]['replica']

                cache_writer.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s \n"
                                   % (filename, dataset_id, version, download_url, variable, variable_cf_name, variable_long_name, variable_units, \
                      experiment_family, product, filesize, forcings, checksum_type, checksum, tracking_id, index_node, replica))


    return


def get_ensembles(node, project, var, table, expt, model, latest, distrib):
    """
    Get a list of all models for a given variable for each experiment and
    store them in a dictionary

    :param node: node at which to test
    :param project: search project
    :param var: variable of interest
    :param table: cmor_table associated with variable
    :param expt: experiment
    :param model: model
    :param latest: latest True/False search
    :param distrib: distributed search

    :return: List of ensembles satisfying the above search criteria
    """

    url = "https://%(node)s/esg-search/search?type=File" \
          "&project=%(project)s&experiment=%(expt)s&variable=%(var)s&cmor_table=%(table)s" \
          "&latest=%(latest)s&distrib=%(distrib)s&model=%(model)s&facets=ensemble&format=application%%2Fsolr%%2Bjson&limit=1000" \
          % vars()

    resp = requests.get(url)
    json = resp.json()

    ens = json["facet_counts"]["facet_fields"]['ensemble']
    ensembles = dict(itertools.izip_longest(*[iter(ens)] * 2, fillvalue=""))

    return ensembles.keys()


def get_models(node, project, var, table, expt, latest, distrib):
    """
    Get a list of all models for a given variable for each experiment and
    store them in a dictionary

    :param node: node at which to test
    :param project: search project
    :param var: variable of interest
    :param table: cmor_table associated with variable
    :param expt: experiments
    :param latest: latest True/False search
    :param distrib: distributed search

    :return: List of models satisfying the above search criteria
    """

    url = "https://%(node)s/esg-search/search?type=File" \
          "&project=%(project)s&experiment=%(expt)s&variable=%(var)s&cmor_table=%(table)s" \
          "&latest=%(latest)s&distrib=%(distrib)s&facets=model&format=application%%2Fsolr%%2Bjson&limit=1000" \
          % vars()

    resp = requests.get(url)
    json = resp.json()

    mods = json["facet_counts"]["facet_fields"]['model']
    models = dict(itertools.izip_longest(*[iter(mods)] * 2, fillvalue=""))

    return models.keys()


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

    mvars = collections.OrderedDict()
    mvars['tas'] = ['Amon']
    mvars['ts'] = ['Amon']
    mvars['tasmax'] = ['Amon']
    mvars['tasmin'] = ['Amon']
    mvars['psl'] = ['Amon']
    mvars['ps'] = ['Amon']
    mvars['uas'] = ['Amon']
    mvars['vas'] = ['Amon']
    mvars['sfcWind'] = ['Amon']
    mvars['hurs'] = ['Amon']
    mvars['huss'] = ['Amon']
    mvars['pr'] = ['Amon']
    mvars['prsn'] = ['Amon']
    mvars['evspsbl'] = ['Amon']
    mvars['tauu'] = ['Amon']
    mvars['tauv'] = ['Amon']
    mvars['hfls'] = ['Amon']
    mvars['hfss'] = ['Amon']
    mvars['rlds'] = ['Amon']
    mvars['rlus'] = ['Amon']
    mvars['rsds'] = ['Amon']
    mvars['rsus'] = ['Amon']
    mvars['rsdt'] = ['Amon']
    mvars['rsut'] = ['Amon']
    mvars['rlut'] = ['Amon']
    mvars['clt'] = ['Amon']
    mvars['mrsos'] = ['Lmon']
    mvars['mrro'] = ['Lmon']
    mvars['snw'] = ['LImon']
    mvars['tos'] = ['Omon']
    mvars['sos'] = ['Omon']
    mvars['zos'] = ['Omon']
    mvars['sic'] = ['OImon']
    mvars['sit'] = ['OImon']
    mvars['snd'] = ['OImon']
    mvars['sim'] = ['OImon']
    mvars['tsice'] = ['OImon']
    mvars['ta'] = ['Amon']
    mvars['ua'] = ['Amon']
    mvars['va'] = ['Amon']
    mvars['hur'] = ['Amon']
    mvars['hus'] = ['Amon']
    mvars['zg'] = ['Amon']

    dvars = collections.OrderedDict()
    dvars['tas'] = ['day']
    dvars['tasmax'] = ['day']
    dvars['tasmin'] = ['day']
    dvars['psl'] = ['day']
    dvars['sfcWind'] = ['day']
    dvars['huss'] = ['day']
    dvars['pr'] = ['day']

    return mvars, dvars

if __name__ == '__main__':
    get_info()
