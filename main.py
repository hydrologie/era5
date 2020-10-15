# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def main():
    import fsspec
    import xarray as xr
    from distributed import Client

    client = Client()

    bucket_era5_land = 's3://era5-atlantic-northeast/zarr/land/reanalysis'

    # Url du serveur contenant le bucket
    client_kwargs = {"endpoint_url": "https://s3.us-east-2.wasabisys.com"}

    store = fsspec.get_mapper(url=bucket_era5_land,
                              client_kwargs=client_kwargs,
                              anon=True)

    # Ouverture du zarr vers dataset (xarray)
    ds = xr.open_zarr(store,
                      consolidated=True,
                      chunks='auto')

    bucket = 's3://era5-atlantic-northeast/zarr/land/test2'
    storage_options = {'endpoint_url': 'https://s3.us-east-2.wasabisys.com'}

    store = fsspec.get_mapper(bucket,
                              profile='wasabi',
                              client_kwargs=storage_options)

    ds.sel(time=slice('1981-01-01', '1981-01-02')).to_zarr(store, consolidated=True)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
