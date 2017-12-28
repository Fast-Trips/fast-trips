def user_class(row_series):
    """
    Defines the user class for this trip list.

    This function takes a single argument, the pandas.Series with person, household and
    trip_list attributes, and returns a user class string.
    """

    if int(row_series["hh_income"])> 35000:
        return "medhighinc"
    return "lowinc"