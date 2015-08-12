YAML = require 'js-yaml'
converter = require('json-2-csv')
{CompositeDisposable} = require 'atom'

unique = (array) ->
  output = {}
  output[array[key]] = array[key] for key in [0...array.length]
  value for key, value of output

isEven = (val) ->
  return val % 2 == 0

module.exports = JsonConverter =
  subscriptions: null

  config:
    jsonIndet:
      title: 'JSON Indent'
      type: 'integer'
      default: 2
    yamlIndet:
      title: 'YAML Indent'
      type: 'integer'
      default: 2
    csvDelimiterField:
      title: 'CSV Delimiter'
      type: 'string'
      default: ','
    csvDelimiterArray:
      title: 'CSV Delimiter Array'
      type: 'string'
      default: ';'
    csvDelimiterWrap:
      title: 'CSV Wrap Values in Quotes'
      type: 'string'
      default: '"'
    elasticIndex:
      title: 'Elasticsearch Index Name'
      type: 'string'
      default: 'blog'
    elasticDocType:
      title: 'Elasticsearch Type Name'
      type: 'string'
      default: 'posts'
    elasticUidField:
      title: 'Elasticsearch Stored UID Field Name in CSV'
      type: 'string'
      default: 'id'
    elasticParentUidField:
      title: 'Elasticsearch Stored Parent UID Field Name in CSV'
      type: 'string'
      default: 'parent'
    elasticExcludeFields:
      title: 'Elasticsearch Exclude fields in CSV'
      type: 'array'
      default: []

  activate: (state) ->
    @subscriptions = new CompositeDisposable
    @subscriptions.add atom.commands.add 'atom-workspace', 'json-converter:csv-to-elasticsearch-bulk-create-format': => @csvToEsBulk(action: 'create')
    @subscriptions.add atom.commands.add 'atom-workspace', 'json-converter:csv-to-elasticsearch-bulk-delete-format': => @csvToEsBulk(action: 'delete')
    @subscriptions.add atom.commands.add 'atom-workspace', 'json-converter:csv-to-elasticsearch-bulk-index-format': => @csvToEsBulk(action: 'index')
    @subscriptions.add atom.commands.add 'atom-workspace', 'json-converter:csv-to-elasticsearch-bulk-update-format': => @csvToEsBulk(action: 'update')

    @subscriptions.add atom.commands.add 'atom-workspace', 'json-converter:csv-to-json': => @csvToJson()
    @subscriptions.add atom.commands.add 'atom-workspace', 'json-converter:json-to-csv': => @jsonToCsv()
    @subscriptions.add atom.commands.add 'atom-workspace', 'json-converter:json-to-yaml': => @jsonToYaml()
    @subscriptions.add atom.commands.add 'atom-workspace', 'json-converter:yaml-to-json': => @yamlToJson()

  deactivate: ->
    @subscriptions.dispose()

  serialize: ->

  csvToJson: ->
    editor = atom.workspace.getActiveTextEditor()
    return unless editor?

    csv = editor.getText()

    options =
      DELIMITER:
        FIELD: atom.config.get('json-converter.csvDelimiterField')
        ARRAY: atom.config.get('json-converter.csvDelimiterArray')
        WRAP: atom.config.get('json-converter.csvDelimiterWrap')

    converter.csv2json(csv, (error, json) ->
      if not error
        atom.workspace.open('').done((newEditor) ->
          newEditor.setGrammar(atom.grammars.selectGrammar('untitled.json'))
          indent = atom.config.get('json-converter.jsonIndet')
          text = JSON.stringify(json, null, indent)
          newEditor.setText(text)
        )
      else
        atom.notifications?.addError('csvToJson: CSV convert error',
          dismissable: true, detail: error)
    , options)

  jsonToCsv: ->
    editor = atom.workspace.getActiveTextEditor()
    return unless editor?

    try
      json = JSON.parse(editor.getText())
    catch error
      atom.notifications?.addError('jsonToCsv: JSON parse error',
        dismissable: true, detail: error.toString())

    options =
      DELIMITER:
        FIELD: atom.config.get('json-converter.csvDelimiterField')
        ARRAY: atom.config.get('json-converter.csvDelimiterArray')
        WRAP: atom.config.get('json-converter.csvDelimiterWrap')

    converter.json2csv(json, (error, csv) ->
      if not error
        atom.workspace.open('').done((newEditor) ->
          newEditor.setGrammar(atom.grammars.selectGrammar('untitled.csv'))
          newEditor.setText(csv)
        )
      else
        atom.notifications?.addError('jsonToCsv: JSON convert error',
          dismissable: true, detail: error)
    , options)

  jsonToYaml: ->
    editor = atom.workspace.getActiveTextEditor()
    return unless editor?

    try
      json = JSON.parse(editor.getText())
    catch error
      atom.notifications?.addError('jsonToYaml: JSON parse error',
        dismissable: true, detail: error.toString())

    atom.workspace.open('').done((newEditor) ->
      newEditor.setGrammar(atom.grammars.selectGrammar('untitled.yaml'))
      indent = atom.config.get('json-converter.yamlIndent')
      text = YAML.safeDump(json, indent: indent)
      newEditor.setText(text)
    )


  yamlToJson: ->
    editor = atom.workspace.getActiveTextEditor()
    return unless editor?

    try
      json = YAML.safeLoad(editor.getText(), schema: YAML.JSON_SCHEMA)
    catch error
      atom.notifications?.addError('yamlToJson: YAML parse error',
        dismissable: true, detail: error.toString())

    atom.workspace.open('').done((newEditor) ->
      newEditor.setGrammar(atom.grammars.selectGrammar('untitled.json'))
      indent = atom.config.get('json-converter.jsonIndet')
      text = JSON.stringify(json, null, indent)
      newEditor.setText(text)
    )

  csvToEsBulk: ({action}={action: 'index'})->
    editor = atom.workspace.getActiveTextEditor()
    return unless editor?

    csv = editor.getText()

    options =
      DELIMITER:
        FIELD: atom.config.get('json-converter.csvDelimiterField')
        ARRAY: atom.config.get('json-converter.csvDelimiterArray')
        WRAP: atom.config.get('json-converter.csvDelimiterWrap')

    converter.csv2json(csv, (error, docs) ->
      if not error
        atom.workspace.open('').done((newEditor) ->
          newEditor.setGrammar(atom.grammars.selectGrammar('untitled.json'))

          metaIndex = atom.config.get('json-converter.elasticIndex')
          metaType = atom.config.get('json-converter.elasticDocType')
          metaId = atom.config.get('json-converter.elasticUidField')
          metaParentId = atom.config.get('json-converter.elasticParentUidField')
          excludeFields = atom.config.get('json-converter.elasticExcludeFields')

          for doc in docs
            docKeys = Object.keys(doc)

            meta = {}
            meta[action] = {}
            meta[action]._index = metaIndex if metaIndex
            meta[action]._type = metaType if metaType
            meta[action]._id = doc[metaId] if metaId in docKeys
            meta[action]._parent = doc[metaParentId] if metaParentId in docKeys

            newEditor.insertText(JSON.stringify(meta) + '\r\n')

            if action in ['index', 'create', 'update']
              for field, value of doc
                delete doc[field] if field in excludeFields

              doc = {"doc": doc} if action is 'update'
              newEditor.insertText(JSON.stringify(doc) + '\r\n')
        )
      else
        atom.notifications?.addError('csvToJson: CSV convert error',
          dismissable: true, detail: error)
    , options)
