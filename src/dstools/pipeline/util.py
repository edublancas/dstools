import base64


def image_bytes2html(data):
    fig_base64 = base64.encodebytes(data)
    img = fig_base64.decode("utf-8")
    html = '<img src="data:image/png;base64,' + img + '"></img>'
    return html
