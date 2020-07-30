class CardinalitySpec:
    """
    https://en.wikipedia.org/wiki/Cardinality_(data_modeling)

    Assumption, there is one base observation and all other specs
    have either a one-to-one or one-to-many cardinality. When sampling,
    first sample from the base observation. For one-to-one sample with certain
    probability to give a chance for the sampled derived observation not to
    have a match with the base obervation (P is computed from the data).
    For one-to-many, repeat the same (prob of having a match) and then a second
    trial to determine how many rows will be sampled for that base observation
    ID. The amont is also determined by the data (maybe a normal distribution
    with empirical mean and std?)
    """
    def __init__(self, specs):
        self.specs = specs
