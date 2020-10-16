from retrying import retry
import fsspec
import xarray as xr
from distributed import Client
import cdsapi
from datetime import date
import os
import pandas as pd


@retry(stop_max_attempt_number=10)
def get_era5(date, bucket, store):
    year = "{:04d}".format(date.year)
    month = "{:02d}".format(date.month)
    day = "{:02d}".format(date.day)

    c = cdsapi.Client()

    name = 'reanalysis-era5-land'
    request = {'format': 'netcdf',
               'variable': [
                   '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
                   '2m_temperature', 'total_evaporation', 'lake_ice_depth',
                   'skin_temperature', 'snow_cover', 'snow_depth_water_equivalent',
                   'snowfall', 'surface_net_solar_radiation', 'surface_runoff',
                   'temperature_of_snow_layer', 'total_precipitation',
                   'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2',
                   'volumetric_soil_water_layer_3', 'snowmelt', 'snow_depth',
               ],
               'area': [63, -96, 40, -52],  # North, West, South, East. Default: global,
               'year': year,
               'month': [
                   month
               ],
               'day': [
                   day
               ],
               'time': [
                   '00:00', '01:00', '02:00',
                   '03:00', '04:00', '05:00',
                   '06:00', '07:00', '08:00',
                   '09:00', '10:00', '11:00',
                   '12:00', '13:00', '14:00',
                   '15:00', '16:00', '17:00',
                   '18:00', '19:00', '20:00',
                   '21:00', '22:00', '23:00'
               ]
               }

    r = c.retrieve(name,
                   request,
                   'tmp.nc')
    ds_out = xr.open_dataset('tmp.nc', engine='scipy')

    #         for name in list(ds_out.keys()):
    #             del ds_out[name].encoding['chunks']
    return ds_out


def process(ds_out, store):
    print('Chunking dataset...')
    ds_out = ds_out.chunk({'time': 8760, 'longitude': 10, 'latitude': 10})

    print('Saving to zarr...')
    ds_out.to_zarr('tmp.zarr', consolidated=True)

    print('Adding new zarr data to existing dataset...')
    ds_add = xr.open_zarr('tmp.zarr', consolidated=True)
    if not store.fs.glob(os.path.join(bucket, '*')):
        ds_add.to_zarr(store, consolidated=True)
    else:
        ds_add.to_zarr(store, mode='a',
                       append_dim='time', consolidated=True)


bucket = 's3://era5-atlantic-northeast/zarr/land/test2'

# Url du serveur contenant le bucket
client_kwargs = {"endpoint_url": "https://s3.us-east-2.wasabisys.com"}
store = fsspec.get_mapper(bucket,
                          profile='wasabi',
                          client_kwargs=client_kwargs)

# Ouverture du zarr vers dataset (xarray)
ds = xr.open_zarr(store,
                  consolidated=True,
                  chunks='auto')

dates = pd.date_range(start=ds.time.max().values,
                      end=date.today(),
                      freq='1D',
                      normalize=True)[1:]

for date_time in dates[0:1]:
    ds_out = get_era5(date_time, bucket, store)
    process(ds_out, store)