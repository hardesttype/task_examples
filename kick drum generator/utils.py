import numpy as np
import matplotlib.pyplot as plt
from pydub import AudioSegment
from pydub import silence
from scipy.io import wavfile
import io


#AudioSegment.converter = "C:\\Storage\\Sample generator\\ffmpeg\\bin\\ffmpeg.exe"
#AudioSegment.ffmpeg = "C:\\Storage\\Sample generator\\ffmpeg\\bin\\ffmpeg.exe"
#AudioSegment.ffprobe = "C:\\Storage\\Sample generator\\ffmpeg\\bin\\ffprobe.exe"


def decode_wav(wav_file: str, mono: bool = False, trim: bool = True) -> np.array:
    """
    Converts wav audio into np.array
    wav_file: path to audio file
    mono: controls the number of channels
    trim: trim silent parts of audio signal
    return: np.array of samples
    """
    seg = AudioSegment.from_wav(wav_file)
    if mono:
        seg = seg.set_channels(1)
    if trim:
        seg = trim_silence(seg)
    channel_sounds = seg.split_to_mono()
    samples = [s.get_array_of_samples() for s in channel_sounds]
    fp_arr = np.array(samples).T.astype(np.float32)
    fp_arr /= np.iinfo(samples[0].typecode).max
    return fp_arr


def encode_wav(data: np.array, sample_rate: int = 44100) -> AudioSegment:
    """
    Converts np.array of samples into AudioSegment
    ### Note: ffmpeg is required ###
    data: np.array of samples
    return: AudioSegment
    """
    wav_io = io.BytesIO()
    wavfile.write(wav_io, sample_rate, data)
    wav_io.seek(0)
    return AudioSegment.from_wav(wav_io)


def graph_spectrogram(wav_file: str or np.array, sample_rate = 44100) -> np.ndarray:
    """
    Draw spectrogram and diagram of a sample and return data of the spectrogram
    wav_file: str -- path to wav file or np.array of samples
    sample_rate: sample rate of audio
    return: np.ndarray
    """
    if type(wav_file) is str:
        samples = decode_wav(wav_file)
    else:
        samples = wav_file
    nfft = 200 # Length of each window segment
    fs = sample_rate # Sampling frequencies
    noverlap = 120 # Overlap between windows
    nchannels = samples.ndim
    fig, ax = plt.subplots(1, 2, figsize = (16, 5))
    # Diagram
    ax[0].plot(samples)
    ax[0].set_xlabel('Time')
    ax[0].set_ylabel('Amplitude')
    ax[0].set_title('Sample diagram')
    # Spectrogram
    if nchannels == 1:
        pxx, freqs, bins, im = ax[1].specgram(samples, nfft, fs, noverlap = noverlap)
    elif nchannels == 2:
        pxx, freqs, bins, im = ax[1].specgram(samples[:,0], nfft, fs, noverlap = noverlap)
    ax[1].set_xlabel('Time, s')
    ax[1].set_ylabel('Frequency, Hz')
    ax[1].set_title('Spectrogram')
    return pxx


def trim_silence(seg: AudioSegment) -> AudioSegment:
    """
    Decrease silent audio signal
    """
    trimmed_segs = silence.split_on_silence(seg,
                                            silence_thresh=-50,
                                            min_silence_len=100,
                                            keep_silence=25)
    return trimmed_segs[0]