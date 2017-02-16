import unittest

import pavics.calendars as ty


class TestTimely(unittest.TestCase):

    def setUp(self):
        self.gregorian_months = {1: 'January', 2: 'February', 3: 'March',
                                 4: 'April', 5: 'May', 6: 'June', 7: 'July',
                                 8: 'August', 9: 'September', 10: 'October',
                                 11: 'November', 12: 'December'}
        self.temperate_seasons = {1: 'Spring', 2: 'Summer', 3: 'Autumn',
                                  4: 'Winter'}
        self.year_cycle = {1: 'Year'}
        self.days28 = list(range(1, 29))
        self.days29 = list(range(1, 30))
        self.days30 = list(range(1, 31))
        self.days31 = list(range(1, 32))
        self.arbitrary_years = [-10000, -4966, -1, 0, 400, 1878, 1900, 2000,
                                2660, 9999]
        self.never_leap_years = [-1774, 1, 890, 1962, 2711]

        # Every day from 1961 to 1975 (inclusive, 360 day per year)
        self.collection_time_2d1 = []
        for yyyy in range(1961, 1776):
            for mm in range(1, 13):
                for dd in range(1, 31):
                    self.collection_time_2d1.append([yyyy, mm, dd])
        # Every january from 1965 to 1970 (inclusive)
        self.collection_period_3d1 = []
        self.collection_period_3d1_inclusive = []
        for yyyy in range(1965, 1971):
            self.collection_period_3d1.append([[yyyy, 1, 1, 0, 0, 0],
                                              [yyyy, 2, 1, 0, 0, 0]])
            self.collection_period_3d1_inclusive.append([1, 0])

    def tearDown(self):
        pass

    def test_calendar_eq(self):
        cal1 = ty.Calendar('some_name', ty.year_cycle, ty.day_in_year,
                           ty.is_leap_feb29)
        cal2 = ty.Calendar('some_name', ty.year_cycle, ty.day_in_year,
                           ty.is_leap_feb29)
        self.assertEqual(cal1, cal2)

    def test_calendar_ne(self):
        cal1 = ty.Calendar('some_name', ty.year_cycle, ty.day_in_year,
                           ty.is_leap_feb29)
        cal2 = ty.Calendar('some_other_name', ty.year_cycle, ty.day_in_year,
                           ty.is_leap_feb29)
        self.assertNotEqual(cal1, cal2)

    def test_cal360_days(self):
        for year in self.arbitrary_years + self.never_leap_years:
            for month in range(1, 13):
                days = ty.Cal360.days_in_cycle(month, year)
                self.assertEqual(days, self.days30)

    def test_cal360_is_leap(self):
        for year in self.arbitrary_years + self.never_leap_years:
            flag_leap = ty.Cal360.is_leap(year)
            self.assertFalse(flag_leap)

    def test_cal360_count_days_in_year(self):
        for year in self.arbitrary_years + self.never_leap_years:
            days_in_year = ty.Cal360.count_days_in_year(year)
            self.assertEqual(days_in_year, 360)

    def test_cal365_days(self):
        for year in self.arbitrary_years + self.never_leap_years:
            days = ty.Cal365.days_in_cycle(1, year)
            self.assertEqual(days, self.days31)
            days = ty.Cal365.days_in_cycle(2, year)
            self.assertEqual(days, self.days28)
            days = ty.Cal365.days_in_cycle(6, year)
            self.assertEqual(days, self.days30)
            days = ty.Cal365.days_in_cycle(12, year)
            self.assertEqual(days, self.days31)

    def test_cal365_is_leap(self):
        for year in self.arbitrary_years + self.never_leap_years:
            flag_leap = ty.Cal365.is_leap(year)
            self.assertFalse(flag_leap)

    def test_cal365_count_days_in_year(self):
        for year in self.arbitrary_years + self.never_leap_years:
            days_in_year = ty.Cal365.count_days_in_year(year)
            self.assertEqual(days_in_year, 365)

    def test_cal366_days(self):
        for year in self.arbitrary_years + self.never_leap_years:
            days = ty.Cal366.days_in_cycle(1, year)
            self.assertEqual(days, self.days31)
            days = ty.Cal366.days_in_cycle(2, year)
            self.assertEqual(days, self.days29)
            days = ty.Cal366.days_in_cycle(6, year)
            self.assertEqual(days, self.days30)
            days = ty.Cal366.days_in_cycle(12, year)
            self.assertEqual(days, self.days31)

    def test_cal366_is_leap(self):
        for year in self.arbitrary_years + self.never_leap_years:
            flag_leap = ty.Cal366.is_leap(year)
            self.assertTrue(flag_leap)

    def test_cal366_count_days_in_year(self):
        for year in self.arbitrary_years + self.never_leap_years:
            days_in_year = ty.Cal366.count_days_in_year(year)
            self.assertEqual(days_in_year, 366)

    def test_caljulian_days(self):
        for year in self.never_leap_years:
            days = ty.CalJulian.days_in_cycle(1, year)
            self.assertEqual(days, self.days31)
            days = ty.CalJulian.days_in_cycle(2, year)
            self.assertEqual(days, self.days28)
            days = ty.CalJulian.days_in_cycle(6, year)
            self.assertEqual(days, self.days30)
            days = ty.CalJulian.days_in_cycle(12, year)
            self.assertEqual(days, self.days31)
        days = ty.CalJulian.days_in_cycle(1, 1964)
        self.assertEqual(days, self.days31)
        days = ty.CalJulian.days_in_cycle(2, 1964)
        self.assertEqual(days, self.days29)
        days = ty.CalJulian.days_in_cycle(6, 1964)
        self.assertEqual(days, self.days30)
        days = ty.CalJulian.days_in_cycle(12, 1964)
        self.assertEqual(days, self.days31)

    def test_caljulian_is_leap(self):
        for year in self.never_leap_years:
            flag_leap = ty.CalJulian.is_leap(year)
            self.assertFalse(flag_leap)
        flag_leap = ty.CalJulian.is_leap(1968)
        self.assertTrue(flag_leap)

    def test_caljulian_count_days_in_year(self):
        for year in self.never_leap_years:
            days_in_year = ty.CalJulian.count_days_in_year(year)
            self.assertEqual(days_in_year, 365)
        days_in_year = ty.CalJulian.count_days_in_year(1972)
        self.assertEqual(days_in_year, 366)

    def test_calproleptic_days(self):
        for year in self.never_leap_years:
            days = ty.CalProleptic.days_in_cycle(1, year)
            self.assertEqual(days, self.days31)
            days = ty.CalProleptic.days_in_cycle(2, year)
            self.assertEqual(days, self.days28)
            days = ty.CalProleptic.days_in_cycle(6, year)
            self.assertEqual(days, self.days30)
            days = ty.CalProleptic.days_in_cycle(12, year)
            self.assertEqual(days, self.days31)
        days = ty.CalProleptic.days_in_cycle(1, 1964)
        self.assertEqual(days, self.days31)
        days = ty.CalProleptic.days_in_cycle(2, 1964)
        self.assertEqual(days, self.days29)
        days = ty.CalProleptic.days_in_cycle(6, 1964)
        self.assertEqual(days, self.days30)
        days = ty.CalProleptic.days_in_cycle(12, 1964)
        self.assertEqual(days, self.days31)
        days = ty.CalProleptic.days_in_cycle(1, 1900)
        self.assertEqual(days, self.days31)
        days = ty.CalProleptic.days_in_cycle(2, 1900)
        self.assertEqual(days, self.days28)
        days = ty.CalProleptic.days_in_cycle(6, 1900)
        self.assertEqual(days, self.days30)
        days = ty.CalProleptic.days_in_cycle(12, 1900)
        self.assertEqual(days, self.days31)
        days = ty.CalProleptic.days_in_cycle(1, 2000)
        self.assertEqual(days, self.days31)
        days = ty.CalProleptic.days_in_cycle(2, 2000)
        self.assertEqual(days, self.days29)
        days = ty.CalProleptic.days_in_cycle(6, 2000)
        self.assertEqual(days, self.days30)
        days = ty.CalProleptic.days_in_cycle(12, 2000)
        self.assertEqual(days, self.days31)

    def test_calproleptic_is_leap(self):
        for year in self.never_leap_years:
            flag_leap = ty.CalProleptic.is_leap(year)
            self.assertFalse(flag_leap)
        flag_leap = ty.CalProleptic.is_leap(1964)
        self.assertTrue(flag_leap)
        flag_leap = ty.CalProleptic.is_leap(1900)
        self.assertFalse(flag_leap)
        flag_leap = ty.CalProleptic.is_leap(2000)
        self.assertTrue(flag_leap)

    def test_calproleptic_count_days_in_year(self):
        for year in self.never_leap_years:
            days_in_year = ty.CalProleptic.count_days_in_year(year)
            self.assertEqual(days_in_year, 365)
        days_in_year = ty.CalProleptic.count_days_in_year(1964)
        self.assertEqual(days_in_year, 366)
        days_in_year = ty.CalProleptic.count_days_in_year(1900)
        self.assertEqual(days_in_year, 365)
        days_in_year = ty.CalProleptic.count_days_in_year(2000)
        self.assertEqual(days_in_year, 366)

    def test_calgregorian_days(self):
        for year in self.never_leap_years:
            days = ty.CalGregorian.days_in_cycle(1, year)
            self.assertEqual(days, self.days31)
            days = ty.CalGregorian.days_in_cycle(2, year)
            self.assertEqual(days, self.days28)
            days = ty.CalGregorian.days_in_cycle(6, year)
            self.assertEqual(days, self.days30)
            days = ty.CalGregorian.days_in_cycle(12, year)
            self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(1, 1964)
        self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(2, 1964)
        self.assertEqual(days, self.days29)
        days = ty.CalGregorian.days_in_cycle(6, 1964)
        self.assertEqual(days, self.days30)
        days = ty.CalGregorian.days_in_cycle(12, 1964)
        self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(1, 1900)
        self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(2, 1900)
        self.assertEqual(days, self.days28)
        days = ty.CalGregorian.days_in_cycle(6, 1900)
        self.assertEqual(days, self.days30)
        days = ty.CalGregorian.days_in_cycle(12, 1900)
        self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(1, 2000)
        self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(2, 2000)
        self.assertEqual(days, self.days29)
        days = ty.CalGregorian.days_in_cycle(6, 2000)
        self.assertEqual(days, self.days30)
        days = ty.CalGregorian.days_in_cycle(12, 2000)
        self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(1, 1100)
        self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(2, 1100)
        self.assertEqual(days, self.days29)
        days = ty.CalGregorian.days_in_cycle(6, 1100)
        self.assertEqual(days, self.days30)
        days = ty.CalGregorian.days_in_cycle(12, 1100)
        self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(1, 1000)
        self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(2, 1000)
        self.assertEqual(days, self.days29)
        days = ty.CalGregorian.days_in_cycle(6, 1000)
        self.assertEqual(days, self.days30)
        days = ty.CalGregorian.days_in_cycle(12, 1000)
        self.assertEqual(days, self.days31)
        days = ty.CalGregorian.days_in_cycle(10, 1582)
        self.assertEqual(days, [1, 2, 3, 4, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                24, 25, 26, 27, 28, 29, 30, 31])

    def test_calgregorian_is_leap(self):
        for year in self.never_leap_years:
            flag_leap = ty.CalGregorian.is_leap(year)
            self.assertFalse(flag_leap)
        flag_leap = ty.CalGregorian.is_leap(1964)
        self.assertTrue(flag_leap)
        flag_leap = ty.CalGregorian.is_leap(1900)
        self.assertFalse(flag_leap)
        flag_leap = ty.CalGregorian.is_leap(2000)
        self.assertTrue(flag_leap)
        flag_leap = ty.CalGregorian.is_leap(1100)
        self.assertTrue(flag_leap)
        flag_leap = ty.CalGregorian.is_leap(1000)
        self.assertTrue(flag_leap)

    def test_calgregorian_count_days_in_year(self):
        for year in self.never_leap_years:
            days_in_year = ty.CalGregorian.count_days_in_year(year)
            self.assertEqual(days_in_year, 365)
        days_in_year = ty.CalGregorian.count_days_in_year(1964)
        self.assertEqual(days_in_year, 366)
        days_in_year = ty.CalGregorian.count_days_in_year(1900)
        self.assertEqual(days_in_year, 365)
        days_in_year = ty.CalGregorian.count_days_in_year(2000)
        self.assertEqual(days_in_year, 366)
        days_in_year = ty.CalGregorian.count_days_in_year(1100)
        self.assertEqual(days_in_year, 366)
        days_in_year = ty.CalGregorian.count_days_in_year(1000)
        self.assertEqual(days_in_year, 366)

    def test_calyearsonly_days(self):
        for year in self.arbitrary_years + self.never_leap_years:
            days = ty.CalYearsOnly.days_in_cycle(1, year)
            self.assertEqual(days, [1])

    def test_calyearsonly_is_leap(self):
        for year in self.arbitrary_years + self.never_leap_years:
            self.assertFalse(ty.CalYearsOnly.is_leap(0))

    def test_calyearsonly_count_days_in_year(self):
        for year in self.arbitrary_years + self.never_leap_years:
            days_in_year = ty.CalYearsOnly.count_days_in_year(year)
            self.assertEqual(days_in_year, 1)

    def test_calmonthsonly_days(self):
        for year in self.arbitrary_years + self.never_leap_years:
            for month in range(1, 13):
                days = ty.CalMonthsOnly.days_in_cycle(month, year)
                self.assertEqual(days, [1])

    def test_calmonthsonly_is_leap(self):
        self.assertRaises(ty.CalendarError, ty.CalMonthsOnly.is_leap, 0)

    def test_calmonthsonly_count_days_in_year(self):
        for year in self.arbitrary_years + self.never_leap_years:
            days_in_year = ty.CalMonthsOnly.count_days_in_year(year)
            self.assertEqual(days_in_year, 12)

    def test_calseasons_days(self):
        for year in self.arbitrary_years + self.never_leap_years:
            for season in range(1, 5):
                days = ty.CalSeasons.days_in_cycle(season, year)
                self.assertEqual(days, [1])

    def test_calseasons_is_leap(self):
        for year in self.arbitrary_years + self.never_leap_years:
            self.assertRaises(ty.CalendarError, ty.CalSeasons.is_leap, 0)

    def test_calseasons_count_days_in_year(self):
        for year in self.arbitrary_years + self.never_leap_years:
            days_in_year = ty.CalSeasons.count_days_in_year(year)
            self.assertEqual(days_in_year, 4)

    def test_cal365nomonths_days(self):
        for year in self.arbitrary_years + self.never_leap_years:
            for month in range(1, 13):
                days = ty.Cal365NoMonths.days_in_cycle(month, year)
                self.assertEqual(days, list(range(1, 366)))

    def test_cal365nomonths_is_leap(self):
        for year in self.arbitrary_years + self.never_leap_years:
            self.assertRaises(ty.CalendarError, ty.Cal365NoMonths.is_leap, 0)

    def test_cal365nomonths_count_days_in_year(self):
        for year in self.arbitrary_years + self.never_leap_years:
            days_in_year = ty.Cal365NoMonths.count_days_in_year(year)
            self.assertEqual(days_in_year, 365)

suite = unittest.TestLoader().loadTestsFromTestCase(TestTimely)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
