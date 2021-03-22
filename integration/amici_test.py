from amarcord.amici.analysis import DataSource
from amarcord.amici.analysis import HitFindingParameters
from amarcord.amici.analysis import HitFindingResults
from amarcord.amici.analysis import PeakSearchParameters
from amarcord.amici.analysis import ingest_cheetah

ingest_cheetah(
    DataSource(run_id=10, number_of_frames=20, tag="test"),
    PeakSearchParameters(
        max_num_peaks=100,
        adc_threshold=25,
        minimum_snr=3.5,
        min_pixel_count=1,
        max_pixel_count=10,
        min_res=0,
        max_res=1000,
        bad_pixel_map_filename="/gpfs/bad_masks/very_bad.h5",
        bad_pixel_map_hdf5_path="/data/data",
        local_bg_radius=4,
        min_peak_over_neighbour=0,
        min_snr_biggest_pix=3.5,
        min_snr_peak_pix=6,
        min_sig=11,
        min_squared_gradient=10000,
        geometry="",
    ),
    HitFindingParameters(min_peaks=10),
    HitFindingResults(
        number_hits=1021, hit_rate=0.1021, result_filename="/tmp/foo/cheetah.cxi"
    ),
)
