import win32com.client as win32
from numpy import ndarray

# analytics_team_emails = ['isabel.czarnecki@mediacom.com', 'jasmine.yeejoybland@mediacom.com', 'moses.galvez@mediacom.com']
analytics_team_emails = ['john.atherton@mediacom.com']


class EmailTemplate():
    def __init__(self):
        pass

    def format(self):
        raise NotImplementedError

    def send(self):
        # to, subject, body, attachments
        if not hasattr(self, 'to') or not hasattr(self, 'subject') or not hasattr(self, 'body'):
            raise ValueError('to, subject, body must all be attributes')
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = self.to
        mail.Subject = self.subject
        mail.HtmlBody = self.body

        if hasattr(self, 'attachments'):
            if isinstance(self.attachments, list) or isinstance(self.attachments, ndarray):
                for filepath in self.attachments:
                    mail.Attachments.Add(filepath)
            else:
                mail.Attachments.Add(self.attachments)

        mail.Send()
        return True


class TriggerTemplate(EmailTemplate):
    def __init__(self, who, begin_date, end_date, filepath):
        super().__init__()
        if who.lower() == 'social':

            emails = ['john.atherton@mediacom.com']
            # emails = ['isabel.czarnecki@mediacom.com', 'jasmine.yeejoybland@mediacom.com', 'moses.galvez@mediacom.com']
            # emails = ['isabel.czarnecki@mediacom.com', 'jasmine.yeejoybland@mediacom.com', 'moses.galvez@mediacom.com', 'AdiSocialPerformance@mediacom.com']
        elif who.lower() == 'programmatic':
            emails = ['john.atherton@mediacom.com']
            # emails = ['isabel.czarnecki@mediacom.com', 'jasmine.yeejoybland@mediacom.com', 'moses.galvez@mediacom.com']
            # emails = ['isabel.czarnecki@mediacom.com', 'jasmine.yeejoybland@mediacom.com', 'moses.galvez@mediacom.com', 'adidas.MediaCom.programmatic@mediacom.com']
        else:
            print('`who` must be either social or programmatic!')
            raise ValueError
        self.to = "; ".join(emails)
        self.begin_date = begin_date
        self.end_date = end_date
        self.attachments = filepath
        self.who = who
        self.format()

    def format(self):
        date_string = '%s-%s-%s / %s-%s-%s' % (
            self.begin_date.month, self.begin_date.day, self.begin_date.year, self.end_date.month, self.end_date.day,
            self.end_date.year)
        self.subject = f'AUTOMATED: {self.who} Triggers {date_string}'
        self.body = f'<p>Hello,\nPlease find attached {self.who} triggers for {date_string}.</p> <p>Note: this is an automated email - however, you may respond and I will receive it.</p><p>Best,</p>'
        sig = """<link href="https://fonts.googleapis.com/css?family=Libre+Franklin:700" rel="stylesheet">
<table>
<tbody>
<tr>
<td>
<table>
<tbody>
<tr>
<td style="font-family: 'Libre Franklin'; font-size: 2em; font-style: bold; padding: 0 12px 0 0 0;" valign="middle">Jack Atherton</td>
</tr>
<tr>
<td>MediaCom</td>
</tr>
<tr>
<td>Digital Analyst, Business Science and Analytics</td>
</tr>
<tr>
<td>MediaCom | The Content + Connections Agency</td>
</tr>
<tr>
<td><a href="mailto:john.atherton@mediacom.com">john.atherton@mediacom.com</a></td>
</tr>
<tr>
<td>12180 Millennium Drive, Suite 440, Playa Vista, CA 90094</td>
</tr>
<tr>
<td><a href="https://mediacom.com">mediacom.com</a></td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>"""
        self.body += sig
