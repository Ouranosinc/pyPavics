import os
import glob
import logging
import random

import numpy as np
import matplotlib.pyplot as plt
import netCDF4


fig = plt.figure(figsize=(8, 6))


def set_logger(log_file=None):
    logger = logging.getLogger()
    if len(logger.handlers):
        if hasattr(logger.handlers[0], 'baseFilename'):
            return logger
    if log_file is None:
        log_file = os.environ['NCVALIDATOR_LOGFILE']
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(message)s')
    logger.addHandler(logging.StreamHandler())
    logger.handlers[-1].setLevel(logging.WARNING)
    return logger


def guess_main_variable(ncdataset):
    # For now, let's just take the variable with highest dimensionality,
    # to refine later
    nd = 0
    for var_name in ncdataset.variables:
        ncvar = ncdataset.variables[var_name]
        if len(ncvar.shape) > nd:
            main_var = var_name
            nd = len(ncvar.shape)
    return main_var


def validate_openable(nc_file, log_file=None):
    set_logger(log_file)
    try:
        nc = netCDF4.Dataset(nc_file, 'r')
        logging.info("{0}: Openable (file_format: {1})".format(
            nc_file, nc.file_format))
        nc.close()
        return True
    except:
        logging.error("{0}: Failed to open file".format(nc_file))
        return False


def validate_temporal_continuity(nc_file, log_file=None):
    set_logger(log_file)
    try:
        nc = netCDF4.Dataset(nc_file, 'r')
        nctime = nc.variables['time']
        if np.diff(nctime[:]).min() <= 0:
            logging.error("{0}: Time not always increasing".format(nc_file))
            return False
        nc.close()
        return True
    except:
        logging.error("{0}: Failed to open file".format(nc_file))
        return False


def validate_calendar_consistency(nc_files, log_file=None):
    set_logger(log_file)
    try:
        for i, nc_file in enumerate(nc_files):
            nc = netCDF4.Dataset(nc_file, 'r')
            if 'time' in nc.variables:
                nctime = nc.variables['time']
                if not hasattr(nctime, 'calendar'):
                    calendar = 'gregorian'
                else:
                    calendar = nctime.calendar
                if i == 0:
                    has_time = True
                    starting_calendar = calendar
                else:
                    if not has_time:
                        log_msg = "{0} & {1}: Time dimension partially present"
                        logging.error(log_msg.format(nc_files[0], nc_file))
                    calendar_match = True
                    if starting_calendar in ['gregorian', 'standard']:
                        if not (calendar in ['gregorian', 'standard']):
                            calendar_match = False
                    elif starting_calendar in ['365_day', 'noleap']:
                        if not (calendar in ['365_day', 'noleap']):
                            calendar_match = False
                    elif starting_calendar in ['366_day', 'all_leap']:
                        if not (calendar in ['366_day', 'all_leap']):
                            calendar_match = False
                    else:
                        if starting_calendar != calendar:
                            calendar_match = False
                    if not calendar_match:
                        log_msg = "{0} & {1}: Different calendars"
                        logging.error(log_msg.format(nc_files[0], nc_file))
            else:
                has_time = False
            nc.close()
    except:
        log_msg = "{0}: Failed to run calendar consistency validation"
        logging.error(log_msg.format(os.path.dirname(nc_files[0])))


def validate_temporal_continuity_across_files(nc_files, log_file=None):
    set_logger(log_file)
    try:
        for i, nc_file in enumerate(nc_files):
            nc = netCDF4.Dataset(nc_file, 'r')
            nctime = nc.variables['time']
            if not hasattr(nctime, 'calendar'):
                calendar = 'gregorian'
            else:
                calendar = nctime.calendar
            if i == 0:
                last_t = nctime[-1]
                starting_time_units = nctime.units
            else:
                if nctime.units != starting_time_units:
                    ncdate = netCDF4.num2date(
                        nctime[0], nctime.units, calendar)
                    t = netCDF4.date2num(ncdate, starting_time_units, calendar)
                    ncdate = netCDF4.num2date(
                        nctime[-1], nctime.units, calendar)
                    next_t = netCDF4.date2num(
                        ncdate, starting_time_units, calendar)
                else:
                    t = nctime[0]
                    next_t = nctime[-1]
                if t <= last_t:
                    log_msg = ("{0} & {1}: Second file starts before (or at) "
                               "the end of first file")
                    logging.error(log_msg.format(nc_files[i-1], nc_file))
                last_t = next_t
    except:
        log_msg = "{0}: Failed to run temporal continuity across file"
        logging.error(log_msg.format(os.path.dirname(nc_files[0])))


def placeholder_map_test(nc_file, log_file=None, ax=None):
    set_logger(log_file)
    try:
        nc = netCDF4.Dataset(nc_file, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        ncvar = nc.variables[guess_main_variable(nc)]
        if ax is None:
            ax = fig.add_subplot(221)
        t = random.randint(0, ncvar.shape[0]-1)
        im = ax.pcolormesh(nclon[...], nclat[...], ncvar[t,:,:])
        ax.set_title("Map at timestep {0}".format(str(t)))
        cb = fig.colorbar(im, ax=ax)
        cb.set_label(ncvar.units)
        nc.close()
    except:
        logging.error("{0}: Failed to draw map".format(nc_file))


def placeholder_timeseries_test(nc_file, log_file=None, ax=None):
    set_logger(log_file)
    try:
        nc = netCDF4.Dataset(nc_file, 'r')
        ncvar = nc.variables[guess_main_variable(nc)]
        if ax is None:
            ax = fig.add_subplot(222)
        y = random.randint(0, ncvar.shape[1]-1)
        x = random.randint(0, ncvar.shape[2]-1)
        im = ax.plot(ncvar[:,y,x])
        ax.set_title("Timeseries at x={0} and y={1}".format(str(x),str(y)))
        ax.set_ylabel(ncvar.units)
        nc.close()
    except:
        logging.error("{0}: Failed to plot timeseries".format(nc_file))


def validate_file(nc_file, log_file=None, openable=True,
                  temporal_continuity=True, sample_map=True,
                  sample_timeseries=True):
    set_logger(log_file)
    # Openable
    if openable:
        validate_openable(nc_file)
    # Temporal continuity
    if temporal_continuity:
        validate_temporal_continuity(nc_file)
    # Sample map
    if sample_map:
        placeholder_map_test(nc_file)
    # Sample timeseries
    if sample_timeseries:
        placeholder_timeseries_test(nc_file)


def validate_files_split_temporally(nc_files, log_file=None, openable=True,
                                    calendars=True, temporal_continuity=True,
                                    sample_map=True, sample_timeseries=True):
    # files must be sorted chronogically
    set_logger(log_file)
    # Sample file
    fn = random.randint(0, len(nc_files)-1)
    # Single file checks
    for i, nc_file in enumerate(nc_files):
        random_sample_map = False
        random_sample_timeseries = False
        if i == fn:
            if sample_map:
                random_sample_map = True
            if sample_timeseries:
                random_sample_timeseries = True
        validate_file(
            nc_file, log_file=log_file, openable=openable,
            temporal_continuity=temporal_continuity,
            sample_map=random_sample_map,
            sample_timeseries=random_sample_timeseries)
    # Calendar consistency
    if calendars:
        validate_calendar_consistency(nc_files)
    # Temporal continuity
    if temporal_continuity:
        validate_temporal_continuity_across_files(nc_files)


def validate_dirs(dirs, extension='', log_file=None, openable=True,
                  calendars=True, temporal_continuity=True, sample_map=True,
                  sample_timeseries=True):
    set_logger(log_file)
    for one_dir in dirs:
        nc_files = glob.glob(os.path.join(one_dir, "*{0}".format(extension)))
        nc_files.sort()
        log_dir = os.path.dirname(log_file)
        fig_path = os.path.join(
            log_dir, one_dir.replace('/','.').lstrip('.') + '.png')
        validate_files_split_temporally(
            nc_files, log_file=log_file, openable=openable,
            calendars=calendars, temporal_continuity=temporal_continuity,
            sample_map=sample_map, sample_timeseries=sample_timeseries)
        fig.savefig(fig_path, format='png')
        fig.clf()


def validate_walk(dirs, extension='', log_file=None, openable=True,
                  calendars=True, temporal_continuity=True):
    # will os.walk every directory (no need to add subdirectories manually)
    for one_dir in dirs:
        for root, dirs, files in os.walk(dirs):
            if not files:
                continue
            files.sort()
            validate_files_split_temporally(
                files, log_file=log_file, openable=openable,
                calendars=calendars, temporal_continuity=temporal_continuity)
