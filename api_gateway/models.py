from django.db import models

class QueryLog(models.Model):
    query = models.TextField()
    generated_sql = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
