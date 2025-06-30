---
title: 'Terragon: A Unified Framework for Earth Observation Data Cube Generation'
tags:
  - Python
  - remote-sensing
  - earth-observation
authors:
  - name: Adrian Höhl^[corresponding author]
    orcid: 0000-0003-3380-4489
    affiliation: 1
  - name: Paul Höhn
    orcid: 0009-0002-5953-8887
    affiliation: "1, 2"
  - name: Xiao Xiang Zhu
    orcid: 0000-0001-5530-3613
    affiliation: "1, 3"

affiliations:
 - name: Chair of Data Science in Earth Observation, Technical University of Munich
   index: 1
 - name: Remote Sensing Technology Institute (IMF), German Aerospace Center (DLR)
   index: 2
 - name: Munich Center for Machine Learning
   index: 3
date: 30 June 2025
bibliography: paper.bib

---

# Summary
Terragon (Earth(Poly)gon) is a Python package facilitating access to remote sensing and Earth observation data from multiple sources. Its goal is to unify the process of downloading data in a simple and efficient manner. While existing tools focus on specific satellites or data providers, Terragon offers a more flexible solution. The package offers a consistent way to search, filter, and download data from various data sources. It utilizes a polygon format to define the region of interest and creates a spatio-temporal data cube [@mahechaEarthSystem2020] of rasterized data in the xarray Dataset format, as illustrated in Figure \ref{workflow}. Additionally, it ensures the alignment of projections and resolutions, organizing the data according to the selected resolution and coordinate reference system.

![Terragon Workflow \label{workflow}](image.pdf){height="300pt"}

Currently, several common data providers are supported, including Google Earth Engine [@GORELICK201718], Planetary Computer [@microsoft_open_source_2022_7261897], Copernicus Data Space Ecosystem [@ecosystemCopernicusData2024] and Alaska Satellite Facility [@ASFHome]. The goal is to further develop and maintain this tool by incorporating additional data providers and implementing processing techniques, such as mosaicking and resampling, based on community needs. The software was leveraged to prepare two large-scale datasets Sen12Landslides [@Sen12Landslides_2025] and CropClimateX [@cropclimatex_dataset]. Sen12Landslides contains 75,000 landslide annotations and has over 12,000 patches from Sentinel 1 and 2. CropClimateX contains 15,500 small data cube spanning 1527 counties in the USA, it spans multiple sensors, weather and extreme events, soil and terrain features. More projects are in preparation.

Overall, Terragon reduces the resources required for the time-consuming process of accessing and downloading data from various APIs, combining it into a consistent, reusable and cost-effienct framework. 

# Statement of need
The number of active earth-observation satellites is increasing [@SatelliteDatabase], and platforms like Sentinel-2 are generating vast amounts of data. This development has significantly impacted remote sensing analysis by providing access to large-scale datasets, both free and commercial. Simultaneously, there is an increasing demand for analysis-ready data to develop data-driven methods, such as machine learning models. However, the process of gathering this data remains time-consuming. Existing platforms are fragmented, often providing only specific satellites, formats, and costum APIs. As a result, this leads to repeated work and poor compatibility across platforms, highlighting the need for more convenient access and a unified, straightforward framework.

Although many useful tools have already been developed to help people access and process Earth observation data [@Taconet2024; @Chudley2024; @Haan2023; @montero2024cubo; @hogenson2020hyp3], these tools often support only one satellite, product, and API, or are built for specific programming languages like R. This limits their broader applicability, especially for researchers looking to combine data from multiple sources or work within a Python-based scientific stack, where Python has become the standard for machine learning research. Although there is some functional overlap with cubo [@montero2024cubo], our design diverges substantially. Cubo builds fixed-size data cubes around a centroid point using UTM zones, while Terragon supports arbitrary polygons as input and preserves the source CRS, avoiding pixel misalignment and overlaps. We aim to have a cube that closely mimics the shape of the polygon, making it a far more flexible approach. Cubo's design does not support this paradigm and would require rewriting its core architecture. 

Terragon addresses these challenges, allowing users to focus on their applications rather than the technical setup. This simplifies scaling to different platforms and eliminates the need for repeated setups for each data provider. Terragon offers a straightforward and consistent interface for downloading data from various APIs. It produces clean, analysis-ready data cubes that integrate seamlessly into machine learning workflows and other applications.

# Target Audience
Our target audience includes anyone who needs to download Earth observation data, particularly researchers and users with limited financial resources who cannot afford the costs associated with commercial providers or cloud services. Additionally, Terragon is designed for users with limited technical knowledge who prefer not to spend excessive time navigating different APIs to download remote sensing data.

# Documentation
A documentation of the package is available at [readthedocs](https://terragon.readthedocs.io). 

# Acknowledgements
The work of A. Höhl was supported by the project ML4Earth by the German Federal Ministry for Economic Affairs and Climate Action under grant number 50EE2201C. P. Höhn was funded by HELMHOLTZ IMAGING, a platform of the Helmholtz Information & Data Science Incubator under grant number: ZT-1-PF-4-028. The work of X.X. Zhu is also supported by the Munich Center for Machine Learning.

# References