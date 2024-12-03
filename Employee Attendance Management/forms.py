from django import forms

class ExcelUploadForm(forms.Form):
    file1 = forms.FileField(label='Upload Employee File')
    file2 = forms.FileField(label='Upload Punch File')