# Public DICOM imaging sources — 1.0.0

This domain-owned sample package contains four studies, five complete selected series and 513 instance metadata documents extracted from licensed public DICOM objects. It is independent of any persistence strategy. Pixel bytes are not bundled; each original object's public S3 locator and SHA-256 remain in `binaryRef`. Each instance preserves its original DICOM JSON header (excluding PixelData), study/series/SOP identity, file size and validated ContentIndex offsets.

Use explicit variants: `ct` (87 instances), `mr` (19), `mg` (1), or `linked-ct-pet` (406). They stay within the domain loader's bounded batch limits. The CT and PET in the linked variant share the original study UID. Public identifiers and source dates are de-identified. Each collection has a separate sample activation `centerId` namespace; these namespaces represent public collections rather than clinical facilities. No cross-collection patient linkage or diagnosis is inferred.

Sources via NCI Imaging Data Commons / The Cancer Imaging Archive:

- LIDC-IDRI, CC BY 3.0: https://doi.org/10.7937/k9/tcia.2015.lo9ql9sx
- PROSTATEx, CC BY 3.0: https://doi.org/10.7937/k9tcia.2017.murs5cl
- CBIS-DDSM, CC BY 3.0: https://doi.org/10.7937/k9/tcia.2016.7o02s9cy
- Lung-PET-CT-Dx, CC BY 4.0: https://doi.org/10.7937/tcia.2020.nnc2-0461

Retain collection citation, DOI, license and source-instance provenance when using derived assets. The public showcase displays CT lung windows, MR/MG percentile windows and PET intensity previews in BQML. Its PET maximum-intensity overview is a display transformation, not SUV, image registration or fusion. These examples support data exploration, not diagnosis.

```python
from kehrnel.engine.domains.dicom.sample_data import describe_sample, load_sample

metadata = describe_sample("dicom.public-imaging", "1.0.0")
batch = load_sample(
    "dicom.public-imaging", "1.0.0", variant="linked-ct-pet",
    expected_digest=metadata["reference"]["digest"],
)
```

The loader validates manifest/file SHA-256 references, document counts, read limits and path boundaries, preserving BSON dates and original DICOM VR/value semantics. Trusted local callers may pass a domain sample root; HTTP requests must select advertised IDs rather than filesystem paths.

To build a publisher release, ingest these batches through `dicom_ingest`, then `dicom_rule_receive` and `dicom_rule_process` in a disposable Kehrnel environment with the manifest's recommended activation scopes. Define a bounded profile, apply its regular index plan, and execute `dicom_rule_query` for each prepared question. Save source/profile versions, generations, native query pipelines, coverage, matched UIDs and image provenance together. The public frontend serves immutable saved results; a developer workspace can operate the same domain samples with its chosen strategy.

This package covers four modalities and selected classic single-frame SOP classes. It does not qualify enhanced multiframe, ultrasound, whole-slide pathology, SR, SEG or RT research extraction. Those require additional domain samples and SOP-specific profiles. Modality names do not establish clinical conditions.
