import itertools

# def _generate_numeric(numeric_parameter):
#     default = numeric_parameter["default"]
    

# def generate_small_grid(model_parameters):
#     pass

def generate_grid(model_parameters):
    #Iterate over keys and values
    parameter_groups = []
    for key in model_parameters:
        #Generate tuple key,value
        t = list(itertools.product([key], model_parameters[key]))
        parameter_groups.append(t)
    #Cross product over each group
    parameters = list(itertools.product(*parameter_groups))
    #Convert each result to dict
    dicts = [_tuples2dict(params) for params in parameters]
    return dicts

def _tuples2dict(tuples):
    return dict((x, y) for x, y in tuples)

r = generate_grid(RandomForestClassifier)
print r

#Given a key and array, generate [(key, val1), (key, val2), ...]
#def _gen_tuples(key, values):

