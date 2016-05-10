
def user_class(row_series):
    """
    Defines the user class for this trip list.

    This function takes a single argument, the pandas.Series with person, household and
    trip_list attributes, and returns a user class string.
    """
    # print row_series
    if row_series["hh_id"] == "simpson":
        return "not_real"
    return "real"