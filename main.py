# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

def main():
    import fsspec
    import xarray as xr
    from distributed import Client
    import cdsapi
    from datetime import date
    import os
    import pandas as pd

    client = Client()

    bucket = 's3://era5-atlantic-northeast/zarr/land/test2'

    # Url du serveur contenant le bucket
    client_kwargs = {"endpoint_url": "https://s3.us-east-2.wasabisys.com"}
    store = fsspec.get_mapper(bucket,
                              profile='default',
                              client_kwargs=client_kwargs)

    # Ouverture du zarr vers dataset (xarray)
    ds = xr.open_zarr(store,
                      consolidated=True,
                      chunks='auto')

    c = cdsapi.Client()

    dates = pd.date_range(start=ds.time.max().values,
                          end=date.today(),
                          freq='1D',
                          normalize=True)[1:]

    for date in dates[0:1]:
        year = "{:04d}".format(date.year)
        month = "{:02d}".format(date.month)
        day = "{:02d}".format(date.day)

        for attempt in range(10):
            try:
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
                               None)
                with fsspec.open(r.location) as f:
                    ds_out = xr.open_dataset(f, engine='scipy')

                    for name in list(ds_out.keys()):
                        del ds_out[name].encoding['chunks']

                    ds_out = ds_out.chunk({'time': 8760, 'longitude': 10, 'latitude': 10})

                if not store.fs.glob(os.path.join(bucket, '*')):
                    ds_out.to_zarr(store, consolidated=True)
                else:
                    ds_out.to_zarr(store, mode='a',
                                   append_dim='time', consolidated=True)
                print(date)
            except:
                print('e')  # perhaps reconnect, etc.
            else:
                break


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()