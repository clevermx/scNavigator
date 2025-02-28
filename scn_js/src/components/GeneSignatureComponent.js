import React, {Component} from 'react';
import {Form, Radio, TextArea, Button, Icon} from "semantic-ui-react"
import {speciesMapping} from '../utils/Constants';
import _ from 'lodash';
import {caseInsensetiveRegexpFiltering} from "../utils/Utils";
import {FilterLabel} from "../utils/Utils"
import ReactTable from "react-table";


export default class GeneSignatureComponent extends Component {

    render() {

        const geneSignatureTableColumns = [
            {
                Header: "Name",
                accessor: 'name',
                Filter: FilterLabel("~"),
                filterMethod: caseInsensetiveRegexpFiltering,
                width: 180
            }, {
                Header: "Show Enrichment",
                accessor: 'token',
                Cell: props =>  {
                    return <button onClick={() => {this.props.showGeneSignature(props.value, this.props.latestQuerySymbol)}}
                                   style={{cursor:'pointer'}}>Show enrichment</button>;

                },
                width: 150
            }, {
                Header: "External link",
                accessor: "link",
                Cell: props =>  {
                    return !_.isUndefined(props.value) ?
                        <a href={props.value} target={"_blank"}> <Icon name={"linkify"} size={"tiny"}/> </a> : <> </>;

                },
                width: 50
            },{
                Header: "Title",
                accessor: 'title',
                Filter: FilterLabel("~"),
                filterMethod: caseInsensetiveRegexpFiltering
            }, {
                Header: "Adjusted p value",
                accessor: 'adjPvalue',
                Cell: props => props.value.toPrecision(3),
                Filter: FilterLabel("< 1e-"),
                filterMethod: (filter, row) => row[filter.id] < Math.pow(10, -parseFloat(filter.value)),
                width: 100
            }, {
                Header: "Module size",
                accessor: 'moduleSize',
                width: 50
            }, {
                Header: "Intersection size",
                accessor: 'intersectionSize',
                width: 50
            }];

        let hasResults = _.has(this.props.searchResults, "success");
        let resultsBlock = (<div />);
        if (hasResults) {
            let searchResult = this.props.searchResults.result;
            if (this.props.searchResults.success) {
                let resultsToShow = _.map(searchResult.enrichmentResultItems,
                    (x) => {
                        let ownValues = _.pick(x, ["datasetId", "adjPvalue", "intersectionSize", "moduleSize"]);
                        let extension = searchResult.meta[ownValues["datasetId"]];
                        return _.merge(ownValues, extension);
                    });
                resultsBlock = <ReactTable
                    filterable
                    data={resultsToShow}
                    defaultPageSize={10}
                    columns={geneSignatureTableColumns}
                />
            } else {
                resultsBlock = <div>Something went wrong: {this.props.searchResults.errors}</div>
            }
        }

        return(
            <Form loading={this.props.searchLoading}
                    onSubmit={() => this.props.submitGeneSignatureForm(this.props.speciesFrom,
                        this.props.speciesTo, this.props.searchField)}>
                <Form.Field>
                    Selected species of gene set
                </Form.Field>
                <Form.Group>
                    <Form.Field>
                        <Radio
                            label={speciesMapping["mm"]}
                            name='radioGroupFrom'
                            value='mm'
                            checked={this.props.speciesFrom === 'mm'}
                            onChange={() => this.props.changeInput("speciesFrom", "mm")}
                        />
                    </Form.Field>
                    <Form.Field>
                        <Radio
                            label={speciesMapping["hs"]}
                            name='radioGroupFrom'
                            value='hs'
                            checked={this.props.speciesFrom === 'hs'}
                            onChange={() => this.props.changeInput("speciesFrom", "hs")}
                        />
                    </Form.Field>
                    <Form.Field>
                        <Radio
                            label={speciesMapping["rn"]}
                            name='radioGroupFrom'
                            value='rn'
                            checked={this.props.speciesFrom === 'rn'}
                            onChange={() => this.props.changeInput("speciesFrom", "rn")}
                        />
                    </Form.Field>
                </Form.Group>
                <Form.Field>
                    Selected species of dataset (if it's different from gene set we will use orthology to convert genes)
                </Form.Field>
                <Form.Group>
                    <Form.Field>
                        <Radio
                            label={speciesMapping["mm"]}
                            name='radioGroupTo'
                            value='mm'
                            checked={this.props.speciesTo === 'mm'}
                            onChange={() => this.props.changeInput("speciesTo", "mm")}
                        />
                    </Form.Field>
                    <Form.Field>
                        <Radio
                            label={speciesMapping["hs"]}
                            name='radioGroupTo'
                            value='hs'
                            checked={this.props.speciesTo === 'hs'}
                            onChange={() => this.props.changeInput("speciesTo", "hs")}
                        />
                    </Form.Field>
                    <Form.Field>
                        <Radio
                            label={speciesMapping["rn"]}
                            name='radioGroupTo'
                            value='rn'
                            checked={this.props.speciesTo === 'rn'}
                            onChange={() => this.props.changeInput("speciesTo", "rn")}
                        />
                    </Form.Field>
                </Form.Group>
                <Form.Field>
                    Paste genes in Symbol/Entrez/Ensembl or Refseq format below.
                </Form.Field>
                <Form.Field>
                    <TextArea placeholder='Paste genes here'
                              name={"geneSearchField"}
                              value={this.props.searchField}
                              onChange={(e, { name, value }) => this.props.changeInput("searchField", value)} />
                </Form.Field>
                <Button type='submit'>Submit</Button>
                <br />< br />
                {resultsBlock}

            </Form>
        )
    }
}

