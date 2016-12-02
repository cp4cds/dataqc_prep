# import statements
from pyesgf.search import SearchConnection
import dateutil.parser as dt
import collections, os, timeit


def check_availability():
    """
    Main program
    Run check of data availability for all monthly variables
    :return: output to file
    """
    # SET CONSTRAINTS
    distrib = False


    all_vars = define_all_variables()
    core_vars = ['tas', 'tasmax', 'tasmin', 'ps', 'uas', 'vas', 'pr']
    tri_core_expts=['historical', 'piControl', 'rcp45']
    all_core_expts=['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']

    # set esgf-pyclient connection
    conn = SearchConnection('http://esgf-index1.ceda.ac.uk/esg-search', distrib=distrib)
    ctx_cmip5 = conn.new_context(project='CMIP5', latest=True, replica=True)

    # Loop over two different numbers of ensembles
    for nens in [0, 2]:
        # Loop over two different sets of experiments
        for expts in [tri_core_expts, all_core_expts]:

            filename = filename_constructor(nens, expts)

            # open file to store output
            with open(filename, "w") as models_writer:

                for var, tables in all_vars.items():
                    for table in tables:
                        # For each variable in a given cmor_table
                        # Get the CMIP5 models that have the required number of ensemble members
                        models_by_expt = get_models(ctx_cmip5, var, table, expts, nens)

                        # Get all the models that exist in all the experiments
                        valid_models = check_in_all_models(models_by_expt)

                        # Calculate this volume of data; write to cache/
                        #total_volume = get_data_volume(ctx_cmip5, var, table, expts, valid_models)

                        # Write aggregates to file
                        #models_writer.write("%s, %s, %s, %s\n" % (var, table, len(valid_models), total_volume))
                        print var, table, len(valid_models)
                        models_writer.write("%s, %s, %s\n" % (var, table, len(valid_models)))


def get_data_volume(ctx, var, table, experiments, models):
    """
    Calculate data volumes for a given variable in CMIP5 over
    a set of models and experiments using ESGF-PyClient

    :param ctx:         esgf-pyclient context
    :param var:         CMIP5 variable to calculate volumes for
    :param table:       cmor_table for variable
    :param experiments: list of experiments to calculate volumes over
    :param models:      list of models to calculate volumes over

    :return:            total volume for variable in given table, experiments and models in GB
    """

    # TODO amend so if file exists read in data volume
    #cache_dir = "cache"
    #if not os.path.isdir(cache_dir):
    #    os.mkdir(cache_dir)

    #uscore = '_'
    volume = 0
    for expt in experiments:
        for model in models:
            # Constrain search criteria
            this_ctx = ctx.constrain(variable=var, cmor_table=table, experiment=expt, model=model)
            # Check for datasets and calculate volume
    #        print var, table, expt, model, volume
            for ds in this_ctx.search():
    #            cache_file = os.path.join(cache_dir, var + uscore + table + uscore + expt + uscore + model + uscore + ds.dataset_id + ".sizes")

    #            with open(cache_file, "w") as cache_writer:
                    # Retrieve all file sizes
                    files = ds.file_context().search()
                    for file in files:
                        volume += file.size
   #                     cache_writer.write("%s:%s\n" % (file.filename, file.size))

    return volume / (1024**3)


def get_models(ctx_cmip5, var, table, expts, nens):
    """
    Get a list of all models for a given variable for each experiment and store them in a dictionary

    :param ctx_cmip5:   a esgf-pyclient context of all cmip5
    :param var:         variable to test
    :param table:       corresponding cmor_table for variable
    :param expts:       list of experiments
    :param nens:        number of ensemble members as minimum requirement

    :return:            A dictionary of experiments and models which have at least nens ensemble members
    """

    result = {}
    for expt in expts:
        ctx = ctx_cmip5.constrain(variable=var, cmor_table=table, experiment=expt)
        mods = []
        for mod in ctx.facet_counts['model'].keys():
            this_ctx = ctx.constrain(model=mod)
            if len(this_ctx.facet_counts['ensemble']) > nens:
                # Why does this not work?
                # result[expt] = mods.append(mod)
                mods.append(mod)
                result[expt] = mods
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
        file_ens_string = 'any_ensembles.txt'
    if nens == 2:
        file_ens_string = 'three_ensembles.txt'

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
