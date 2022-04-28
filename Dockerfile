FROM mambaorg/micromamba:0.22.0

# Pruned run for testing/debugging?
ENV ZARRFROMESGF_PRUNE=FALSE

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yaml /tmp/env.yaml
RUN micromamba install -y -f /tmp/env.yaml && \
    micromamba clean --all --yes
COPY --chown=$MAMBA_USER:$MAMBA_USER mysearch.py /opt/src/mysearch.py
COPY --chown=$MAMBA_USER:$MAMBA_USER zarr_from_esgf.py /opt/src/zarr_from_esgf.py

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh", "python", "/opt/src/zarr_from_esgf.py"]
