import matplotlib.pyplot as plt
import numpy as np


def visualize_sat_images(da, gdf, bands):
    assert 0 < len(bands) < 4  # up to 3 bands

    fig, axs = plt.subplots(1, len(da.time), figsize=(5 * len(da.time), 5))

    if not isinstance(axs, np.ndarray):
        axs = np.array([axs])

    for i, ax in enumerate(axs):
        subset = da.isel(time=i)
        if len(bands) == 1:
            subset[bands[0]].plot.imshow(robust=True, ax=ax, cmap='gray')
        else:
            subset[bands].to_array().plot.imshow(robust=True, ax=ax)

        gdf.plot(ax=ax, facecolor="none", edgecolor="red")
        ax.set_title(str(subset.time.dt.strftime("%Y-%m-%d").values))
        ax.set_axis_off()
        ax.autoscale()

    plt.tight_layout()
    plt.show()
