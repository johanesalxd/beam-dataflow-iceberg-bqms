import setuptools

setuptools.setup(
    name='pubsub-to-bigquery',
    version='0.1.0',
    install_requires=[
        "apache-beam[gcp]>=2.67.0",
        "google-cloud-pubsub>=2.31.1",
        "google-cloud-bigquery>=3.37.0",
    ],
    packages=setuptools.find_packages(),
    author='johanesalxd',
    author_email='17249308+johanesalxd@users.noreply.github.com',
    description='Apache Beam pipeline to read from PubSub and write to BigQuery',
)
