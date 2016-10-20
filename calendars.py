"""
===============
python-calendar
===============

Calendar definitions

"""

gregorian_months = {1:'January',2:'February',3:'March',4:'April',5:'May',
                    6:'June',7:'July',8:'August',9:'September',10:'October',
                    11:'November',12:'December'}

temperate_seasons = {1:'Spring',2:'Summer',3:'Autumn',4:'Winter'}

class CalendarError(Exception):
    pass

##################
# Cycles in year #
##################

def months_of_gregorian_calendar(year=0):
    """Months of the Gregorian calendar.

    Parameters
    ----------
    year : int, optional
        (dummy value).

    Returns
    -------
    out : list of int
        months of the year.

    Notes
    -----
    Appropriate for use as 'year_cycles' function in :class:`Calendar`.
    This module has a built-in calendar with months only:
    :data:`CalMonthsOnly`.

    """

    return range(1,13)

def temperate_seasons(year=0):
    """Temperate seasons.

    Parameters
    ----------
    year : int, optional
        (dummy value).

    Returns
    -------
    out : list of int
        seasons of the year.

    Notes
    -----
    Appropriate for use as 'year_cycles' function in :class:`Calendar`.
    This module has a built-in calendar with seasons only:
    :data:`CalSeasons`.

    """

    return range(1,5)

def year_cycle(year=0):
    """Year cycle.

    Parameters
    ----------
    year : int, optional
        (dummy value).

    Returns
    -------
    out : list of int
        single element [0].

    Notes
    -----
    Appropriate for use as 'year_cycles' function in :class:`Calendar`,
    this allows to essentially have a direct division of the years in
    days, without months, weeks or other subdivisions.
    For example, see built-in calendar :data:`Cal365NoMonths`.

    """

    return [0]

#################
# Days in cycle #
#################

def days_in_month_360(month=0,year=0):
    """Days of the month (360 days calendar).

    Parameters
    ----------
    month : int, optional
        (dummy value).
    year : int, optional
        (dummy value).

    Returns
    -------
    out : list of int
        days of the month.

    Notes
    -----
    Appropriate for use as 'days_in_cycle' function in :class:`Calendar`.
    This module has a built-in 360 days calendar with months:
    :data:`Cal360`.

    """

    return range(1,31)

def days_in_month_365(month,year=0):
    """Days of the month (365 days calendar).

    Parameters
    ----------
    month : int
        numerical value of the month (1 to 12).
    year : int, optional
        (dummy value).

    Returns
    -------
    out : list of int
        days of the month.

    Notes
    -----
    Appropriate for use as 'days_in_cycle' function in :class:`Calendar`.
    This module has a built-in 365 days calendar with months:
    :data:`Cal365`.

    """

    days_in_months = [31,28,31,30,31,30,31,31,30,31,30,31]
    return range(1,days_in_months[month-1]+1)

def days_in_month_366(month,year=0):
    """Days of the month (366 days calendar).

    Parameters
    ----------
    month : int
        numerical value of the month (1 to 12).
    year : int, optional
        (dummy value).

    Returns
    -------
    out : list of int
        days of the month.

    Notes
    -----
    Appropriate for use as 'days_in_cycle' function in :class:`Calendar`.
    This module has a built-in 366 days calendar with months:
    :data:`Cal366`.

    """

    days_in_months = [31,29,31,30,31,30,31,31,30,31,30,31]
    return range(1,days_in_months[month-1]+1)

def days_in_month_julian(month,year):
    """Days of the month (Julian calendar).

    Parameters
    ----------
    month : int
        numerical value of the month (1 to 12).
    year : int

    Returns
    -------
    out : list of int
        days of the month.

    Notes
    -----
    Leap year every 4 years.
    Appropriate for use as 'days_in_cycle' function in :class:`Calendar`.
    This module has a built-in julian calendar: :data:`CalJulian`.

    """

    if (year % 4) == 0:
        return days_in_month_366(month,year)
    else:
        return days_in_month_365(month,year)

def days_in_month_proleptic_gregorian(month,year):
    """Days of the month (Proleptic gregorian calendar).

    Parameters
    ----------
    month : int
        numerical value of the month (1 to 12).
    year : int

    Returns
    -------
    out : list of int
        days of the month.

    Notes
    -----
    Leap year every 4 years, except every 100 years,
    but still every 400 years.
    Appropriate for use as 'days_in_cycle' function in :class:`Calendar`.
    This module has a built-in proleptic gregorian calendar:
    :data:`CalProleptic`.

    """

    if ((year % 100) == 0) and ((year % 400) != 0):
        return days_in_month_365(month,year)
    else:
        return days_in_month_julian(month,year)

def days_in_month_gregorian(month,year):
    """Days of the month (Gregorian calendar).

    Parameters
    ----------
    month : int
        numerical value of the month (1 to 12).
    year : int

    Returns
    -------
    out : list of int
        days of the month.

    Notes
    -----
    Leap year every 4 years, except every 100 years,
    but still every 400 years.
    Transition to Julian calendar before 1582.
    October 5 to October 14 of 1582 do not exist.
    Appropriate for use as 'days_in_cycle' function in :class:`Calendar`.
    This module has a built-in gregorian calendar: :data:`CalGregorian`.

    """

    if (year > 1582) or ((year == 1582) and (month > 10)):
        return days_in_month_proleptic_gregorian(month,year)
    elif (year == 1582) and (month == 10):
        return [1,2,3,4,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]
    else:
        return days_in_month_julian(month,year)

def days_in_year_365(cycle=0,year=0):
    """Days of the year (365 days calendar).

    Parameters
    ----------
    cycle : int, optional
        (dummy value).
    year : int, optional
        (dummy value).

    Returns
    -------
    out : list of int
        365 days of the year.

    Notes
    -----
    Appropriate for use as 'days_in_cycle' function in :class:`Calendar`.
    This module has a built-in 365 day calendar: :data:`Cal365NoMonths`.

    """

    return range(1,366)

def day_in_year(cycle=0,year=0):
    """Single day in a year.

    Parameters
    ----------
    cycle : int, optional
        (dummy value).
    year : int, optional
        (dummy value).

    Returns
    -------
    out : list of int
        day of the month (1).

    Notes
    -----
    Appropriate for use as 'days_in_cycle' function in :class:`Calendar`,
    this is mostly a dummy function to avoid having to define how days
    behave. For example, see built-in calendar :data:`CalYearsOnly`.

    """

    return [1]

###################
# Leap year check #
###################

def is_leap_feb29(year,calendar):
    """Check for leap year (last day of February is the 29th).

    Parameters
    ----------
    year : int
    calendar : Calendar

    Returns
    -------
    out : bool
        True if the given year is a leap year, False otherwise.

    Notes
    -----
    Appropriate for use as 'fn_is_leap' function in :class:`Calendar`.

    """

    days_in_cycle = calendar.days_in_cycle(2,year)
    if days_in_cycle[-1] == 29:
        return True
    else:
        return False





class Calendar:
    """Calendar definition.

    The calendar describes the relation between years, months/seasons
    (or other types of year subdivision), and days. Defining the concept of
    leap year is also possible.

    """

    def __init__(self,alias,cycles_in_year,days_in_cycle,fn_is_leap=None):
        """Initialize calendar.

        Parameters
        ----------
        alias : str
            name given to the calendar, this is also the identifier used
            to test whether two calendars are the same, so this should
            be unique to each defined calendar.
        year_cycles : function(year)
            returns a list of the year cycles.
        days_in_cycle : function(cycle,year)
            returns a list of the days in the cycle.
        fn_is_leap : function(year,calendar), optional
            returns True for leap years, False otherwise.

        """

        self.alias = alias
        self.cycles_in_year = cycles_in_year
        self.days_in_cycle = days_in_cycle
        self.fn_is_leap = fn_is_leap

    def __str__(self):
        return self.alias

    def __eq__(self, other):
        if self.alias == other.alias:
            return True
        else:
            return False

    def __ne__(self, other):
        if self.alias != other.alias:
            return True
        else:
            return False

    def is_leap(self, year):
        """Check if a given year is a leap year.

        Parameters
        ----------
        year : int

        Returns
        -------
        out : bool
            True if the given year is a leap year, False otherwise.

        """

        if self.fn_is_leap is None:
            warp = (self.alias,)
            msg = "Leap year concept not defined for '%s' calendar." % warp
            raise CalendarError(msg)
        if self.fn_is_leap(year,self):
            return True
        else:
            return False

    def count_cycles_in_year(self, year):
        """Count the number of cycles in a year.

        Parameters
        ----------
        year : int

        Returns
        -------
        out : int
            number of cycles in the year.

        """

        return len(self.cycles_in_year(year))

    def count_days_in_cycle(self, cycle, year):
        """Count the number of days in a cycle.

        Parameters
        ----------
        cycle : int
        year : int

        Returns
        -------
        out : int
            number of days in the cycle.

        """

        return len(self.days_in_cycle(cycle,year))

    def count_days_in_year(self, year):
        """Count the number of days in a year.

        Parameters
        ----------
        year : int

        Returns
        -------
        out : int or float('inf')
            number of days in the year.

        """

        year_cycles = self.cycles_in_year(year)
        days_in_year = 0
        for cycle in year_cycles:
            days_in_year += len(self.days_in_cycle(cycle,year))
        return days_in_year

######################
# Built-in calendars #
######################

Cal360 = Calendar('360_day',months_of_gregorian_calendar,days_in_month_360,
                  is_leap_feb29)
Cal365 = Calendar('noleap',months_of_gregorian_calendar,days_in_month_365,
                  is_leap_feb29)
Cal366 = Calendar('all_leap',months_of_gregorian_calendar,days_in_month_366,
                  is_leap_feb29)
CalJulian = Calendar('julian',months_of_gregorian_calendar,days_in_month_julian,
                     is_leap_feb29)
CalProleptic = Calendar('proleptic_gregorian',months_of_gregorian_calendar,
                        days_in_month_proleptic_gregorian,is_leap_feb29)
CalGregorian = Calendar('gregorian',months_of_gregorian_calendar,
                        days_in_month_gregorian,is_leap_feb29)
CalYearsOnly = Calendar('years_only',year_cycle,day_in_year,is_leap_feb29)
CalMonthsOnly = Calendar('months_only',months_of_gregorian_calendar,day_in_year)
CalSeasons = Calendar('seasons',temperate_seasons,day_in_year)
Cal365NoMonths = Calendar('365_days_no_months',year_cycle,days_in_year_365)

def calendar_from_alias(calendar_alias):
    """Get a Calendar object from its alias.

    Parameters
    ----------
    calendar_alias : str

    Returns
    -------
    out : Calendar object

    Notes
    -----
    This is mostly a mapping from the calendars of the CF conventions
    to built-in calendars.

    """

    if calendar_alias == '360_day':
        return Cal360
    elif calendar_alias in ['noleap','365_day']:
        return Cal365
    elif calendar_alias in ['all_leap','366_day']:
        return Cal366
    elif calendar_alias == 'julian':
        return CalJulian
    elif calendar_alias == 'proleptic_gregorian':
        return CalProleptic
    elif calendar_alias in ['gregorian','standard']:
        return CalGregorian
    elif calendar_alias == 'years_only':
        return CalYearsOnly
    elif calendar_alias == 'months_only':
        return CalMonthsOnly
    elif calendar_alias == 'seasons':
        return CalSeasons
    elif calendar_alias == '365_days_no_months':
        return Cal365NoMonths
    else:
        raise CalendarError("Unknown calendar: %s." % (calendar_alias,))
