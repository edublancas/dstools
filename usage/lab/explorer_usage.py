from dstools import ExperimentExplorer

# load everything
explorer = ExperimentExplorer()
# just load results from my_experiment_a
explorer = ExperimentExplorer('my_experiment_a')
# load results from my_experiment_a and my_experiment_b
explorer = ExperimentExplorer(['my_experiment_a', 'my_experiment_b'])

# compute new metric for every model
explorer.apply(lambda m: m.compute_new_metric)
# store this new metric for every model affected
explorer.save()

# after plotting, analyzing results, I want to get the
# trained model
model = explorer.get('some_id')
metric = model.compute_metric()
print 'metric is {}'.format(metric)

# the problem is: should I pickle models? I should NOT pickle everything
# buf it logger is smart enoigh I may be able to just pickle the top models
# another option is to just re-train the model...
# independent of the options the API should be transparent for the user
# since he does not need to know and just be able to recover the object
# - problem with re-training: I need the data. Assuming the data is still the
# same I can do that, but if the numbers have changed and the columns
# are named the same I'm gonna have a baaad time