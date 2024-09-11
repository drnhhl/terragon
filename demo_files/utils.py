import matplotlib.pyplot as plt

def visualize_sat_images(da, gdf, bands):
    assert 0 < len(bands) < 4 # up to 3 bands
    
    fig, axs = plt.subplots(1, len(da.time), figsize=(5, 5))

    for i, ax in enumerate(axs):
        da.isel(time=i)[bands].to_array().plot.imshow(robust=True, ax=ax)
        gdf.plot(ax=ax, facecolor='none', edgecolor='red')
        ax.set_title(da.time[i].dt.strftime('%Y-%m-%d').values)
        ax.set_axis_off()
        ax.autoscale()
    fig.show()