import win32com.client as win32


def autofit_columns(filename):
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(filename)

    for sheet in wb.Sheets:
        sheet.Columns.AutoFit()
        wb.Save()
    excel.Application.Quit()
    return True
