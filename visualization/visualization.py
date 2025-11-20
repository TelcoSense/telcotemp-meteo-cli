import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import os
import logging

backend_logger = logging.getLogger("backend_logger")


def map_plotting(
    grid_x, grid_y, grid_z, czech_rep, image_name, config, show_boundary=False
):
    """
    Plots interpolated temperature map for the given grid and saves it as PNG.
    - Uses custom colormap and levels from config.
    - Optionally draws country boundary.
    - Automatically sets color scale based on median value.
    - Saves image to configured directory.
    """
    visualization_config = config.get_visualization()
    n_levels = visualization_config["n_levels"]
    colormap = visualization_config["colormap"]

    # Default colormap if not provided in config
    if not colormap:
        colormap = [
            (0, "#4E00A6"),
            (1 / 14, "#3600D0"),
            (2 / 14, "#1107F4"),
            (3 / 14, "#0032F7"),
            (4 / 14, "#0467FF"),
            (5 / 14, "#04A3FF"),
            (6 / 14, "#04D27F"),
            (7 / 14, "#1BEC38"),
            (8 / 14, "#63FF00"),
            (9 / 14, "#F4FB0D"),
            (10 / 14, "#FBE316"),
            (11 / 14, "#F7C41B"),
            (12 / 14, "#FC871D"),
            (13 / 14, "#DB4F08"),
            (1, "#A00000"),
        ]

    backend_logger.info("map_plotting: %s", image_name)
    try:
        cmap = mcolors.LinearSegmentedColormap.from_list(
            "custom_colormap", colormap, N=n_levels
        )

        # Set color scale based on median value
        median_value = np.nanmedian(grid_z) - 2
        vmin = int(median_value) - 7
        vmax = int(median_value) + 7

        fig, ax = plt.subplots(figsize=(8, 4), frameon=False)
        c = ax.pcolormesh(
            grid_x,
            grid_y,
            grid_z,
            cmap=cmap,
            shading="auto",
            edgecolor="none",
            vmin=vmin,
            vmax=vmax,
        )
        if show_boundary:
            czech_rep.boundary.plot(ax=ax, linewidth=1, color="black")
        ax.set_axis_off()

        save_dir = visualization_config.get("images_dir", "outputs_web")
        os.makedirs(save_dir, exist_ok=True)
        base_name, ext = os.path.splitext(image_name)
        save_path = os.path.join(save_dir, f"{base_name}_{vmin}_{vmax}{ext}")

        plt.savefig(
            save_path,
            format="png",
            dpi=150,
            transparent=True,
            bbox_inches="tight",
            pad_inches=0,
        )
        plt.close(fig)
        backend_logger.info("Plot saved: %s", save_path)
    except Exception as e:
        backend_logger.exception("Exception in map_plotting: %s", e)
        raise
