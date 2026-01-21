import nibabel as nib
img = nib.load(r"C:\Users\F8944859\Downloads\NIfTI\10246068_CT_1.3.12.2.1107.5.1.4.54209.30000017011908533593700046816.nii.gz")
print(img.header)
print(img.affine)
