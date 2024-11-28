import os
import rdflib
from rdflib.namespace import DC, DCTERMS, RDF, RDFS
from firebase_admin import firestore, credentials, initialize_app
import sys

# Initialize Firebase
cred = credentials.Certificate(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
# initialize_app(cred)
default_app = initialize_app()
db = firestore.client()

# Define namespaces
PGTERMS = rdflib.Namespace('http://www.gutenberg.org/2009/pgterms/')
DCTERMS = rdflib.Namespace('http://purl.org/dc/terms/')
DCAM = rdflib.Namespace('http://purl.org/dc/dcam/')
RDFNS = rdflib.namespace.RDF

def process_rdf_file(rdf_file_path, book_id):
    g = rdflib.Graph()
    try:
        g.parse(rdf_file_path)
    except Exception as e:
        print(f"Error parsing RDF file {rdf_file_path}: {e}")
        return
    
    # Get the ebook URI
    ebook_uri = None
    for s in g.subjects(RDFNS.type, PGTERMS['ebook']):
        ebook_uri = s
        break
    if ebook_uri is None:
        print(f"No ebook found in {rdf_file_path}")
        return
    
    # Initialize metadata dictionary
    metadata = {}
    
    # Extract simple properties
    simple_props = {
        'publisher': DCTERMS.publisher,
        'issued': DCTERMS.issued,
        'rights': DCTERMS.rights,
        'title': DCTERMS.title,
        'alternative_title': DCTERMS.alternative,
        'description': DCTERMS.description,
        'marc508': PGTERMS.marc508,
        'marc520': PGTERMS.marc520,
    }
    for key, prop in simple_props.items():
        value = g.value(ebook_uri, prop)
        if value:
            metadata[key] = str(value)
    
    # Extract downloads
    downloads = g.value(ebook_uri, PGTERMS.downloads)
    if downloads:
        try:
            metadata['downloads'] = int(downloads)
        except ValueError:
            metadata['downloads'] = str(downloads)
    
    # Extract creators
    creators = []
    for creator in g.objects(ebook_uri, DCTERMS.creator):
        agent = {}
        name = g.value(creator, PGTERMS.name)
        if name:
            agent['name'] = str(name)
        birthdate = g.value(creator, PGTERMS.birthdate)
        if birthdate:
            agent['birthdate'] = int(birthdate)
        deathdate = g.value(creator, PGTERMS.deathdate)
        if deathdate:
            agent['deathdate'] = int(deathdate)
        alias = g.value(creator, PGTERMS.alias)
        if alias:
            agent['alias'] = str(alias)
        webpage = g.value(creator, PGTERMS.webpage)
        if webpage:
            agent['webpage'] = str(webpage)
        creators.append(agent)
    if creators:
        metadata['creators'] = creators
    
    # Extract languages
    languages = []
    for lang in g.objects(ebook_uri, DCTERMS.language):
        value = g.value(lang, RDFNS.value)
        if value:
            languages.append(str(value))
    if languages:
        metadata['languages'] = languages
    
    # Extract subjects
    subjects = []
    for subject in g.objects(ebook_uri, DCTERMS.subject):
        value = g.value(subject, RDFNS.value)
        if value:
            subjects.append(str(value))
    if subjects:
        metadata['subjects'] = subjects
    
    # Extract types
    types = []
    for type_ in g.objects(ebook_uri, DCTERMS.type):
        value = g.value(type_, RDFNS.value)
        if value:
            types.append(str(value))
    if types:
        metadata['types'] = types
    
    # Extract bookshelves
    bookshelves = []
    for bookshelf in g.objects(ebook_uri, PGTERMS.bookshelf):
        value = g.value(bookshelf, RDFNS.value)
        if value:
            bookshelves.append(str(value))
    if bookshelves:
        metadata['bookshelves'] = bookshelves
    
    # Extract formats
    formats = []
    for has_format in g.objects(ebook_uri, DCTERMS.hasFormat):
        file_info = {}
        file_uri = has_format
        file_info['url'] = str(file_uri)
        # Extract properties of the file
        extent = g.value(has_format, DCTERMS.extent)
        if extent:
            try:
                file_info['extent'] = int(extent)
            except ValueError:
                file_info['extent'] = str(extent)
        modified = g.value(has_format, DCTERMS.modified)
        if modified:
            file_info['modified'] = str(modified)
        # Extract format types
        format_values = []
        for format_ in g.objects(has_format, DCTERMS.format):
            value = g.value(format_, RDFNS.value)
            if value:
                format_values.append(str(value))
        if format_values:
            file_info['format'] = format_values
        formats.append(file_info)
    if formats:
        metadata['formats'] = formats
    
    # Store metadata in Firestore
    doc_ref = db.collection('repositories').document('projectgutenberg').collection('books').document(str(book_id))
    doc_ref.set(metadata)
    print(f"Stored metadata for book {book_id}")
    
def process_rdf_files_by_ids(base_dir, book_ids):
    for book_id in book_ids:
        rdf_file_path = os.path.join(base_dir, book_id, f'pg{book_id}.rdf')
        if os.path.exists(rdf_file_path):
            process_rdf_file(rdf_file_path, book_id)
        else:
            print(f"RDF file for book ID {book_id} not found at {rdf_file_path}")

if __name__ == "__main__":
    base_dir = './tmp/cache/epub/'
    
    # Option 1: Provide book IDs directly in the script
    book_ids = ['11', '2701', '84', '100', '37106', '25344', '1259', '844', '64317', '2544', '345', '50150']  # Replace with your desired IDs
    
    # Option 2: Get book IDs from command-line arguments
    # Uncomment the following line if you want to pass IDs via command line
    # book_ids = sys.argv[1:]
    
    # Option 3: Read book IDs from a file
    # Uncomment the following lines if you have a file with IDs
    # with open('book_ids.txt', 'r') as f:
    #     book_ids = [line.strip() for line in f.readlines()]
    
    # Process the specified RDF files
    process_rdf_files_by_ids(base_dir, book_ids)
