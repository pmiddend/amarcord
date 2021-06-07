/// <reference types="cypress" />

it('sees both empty tables', () => {
    cy.visit('http://localhost:1234/#/sample');

    // Should only contain the header row
    cy.get('#pucks-table tbody').find('tr').should('have.length', 0)

    cy.get('#crystals-table tbody').find('tr').should('have.length', 0)
});

it('properly adds a new puck', () => {
    cy.visit('http://localhost:1234/#/sample');

   // enter the puck id
   cy.get("#puck-add-puck-id").type("puck1").should('have.value', 'puck1');
   // we have no pucks yet, so button should not be disabled
   cy.get("#puck-add-add-button").not('[disabled]');

   cy.get("#puck-add-add-button").click();

   // Should have one row now!
    cy.get('#pucks-table tbody').find('tr').should('have.length', 1);
    cy.get("#pucks-table tbody tr:first-child td").eq(1).should('contain', 'puck1');
});

// TODO: Test for "add new puck with same ID"
it('properly adds new crystals', () => {
    cy.visit('http://localhost:1234/#/sample');

   // enter the input fields
    cy.get("#input-crystal-id").type("crystal1").should('have.value', 'crystal1');
    cy.get("#crystal-puck-id").select("puck1").should('have.value', 'puck1');
    cy.get("#input-puck-position").clear().type("1").should('have.value', '1');

    // Then submit
   cy.get("#crystal-add-button").click();

   // Should have one row now!
    cy.get('#crystals-table tbody').find('tr').should('have.length', 1);
    cy.get("#crystals-table tbody tr:first-child td").eq(1).should('contain', 'crystal1');
    cy.get("#crystals-table tbody tr:first-child td").eq(2).should('contain', 'puck1');
    cy.get("#crystals-table tbody tr:first-child td").eq(3).should('contain', '1');

    // add second crystal
    cy.get("#input-crystal-id").type("crystal2").should('have.value', 'crystal2');
    cy.get("#crystal-puck-id").select("puck1").should('have.value', 'puck1');
    cy.get("#input-puck-position").clear().type("2").should('have.value', '2');

    // Then submit
    cy.get("#crystal-add-button").click();

    cy.get('#crystals-table tbody').find('tr').should('have.length', 2);

});

it('adds a puck to the dewar table', () => {
    cy.visit('http://localhost:1234/#/beamline');

    cy.get('#dewar-table tbody').find('tr').should('have.length', 0);
    cy.get("#dewar-position").should('have.value', '1');
    cy.get("#puck-id").should('have.value', 'puck1');

    cy.get("#add-dewar-button").click();

    cy.get('#dewar-table tbody').find('tr').should('have.length', 1);
    cy.get("#dewar-table tbody tr:first-child td").eq(1).should('contain', '1');
    cy.get("#dewar-table tbody tr:first-child td").eq(2).should('contain', 'puck1');
});

it('displays the correct diffractions', () => {
    cy.visit('http://localhost:1234/#/beamline');

    cy.get("#dewar-table tbody tr").eq(0).find("td").eq(2).find("a").click();

    cy.get('#diffractions-table tbody').find('tr').should('have.length', 2);

    cy.get('#diffraction-crystal-id').should('have.value', "crystal1");
    cy.get('#diffraction-run-id').should('have.value', "1");
    cy.get('#diffraction-comment').clear().type("foobar");

    cy.on('window:confirm', () => true);
    cy.get("#diffraction-add-button").click();

    cy.get("#diffractions-table tbody").find("tr").eq(0).find("td").eq(5).should("contain", "foobar");
});

it('correctly adds another diffraction run', () => {
    cy.visit('http://localhost:1234/#/beamline');

    cy.get("#dewar-table tbody tr").eq(0).find("td").eq(2).find("a").click();

    cy.get("#diffractions-table tbody").find("tr").eq(0).find("td").eq(0).find("button").click();
    cy.get('#diffraction-crystal-id').should('have.value', "crystal1");
    cy.get('#diffraction-run-id').should('have.value', "2");
    cy.get('#diffraction-comment').clear().type("baz");

    cy.on('window:confirm', () => true);
    cy.get("#diffraction-add-button").click();

    cy.get("#diffractions-table tbody").find("tr").eq(1).find("td").eq(5).should("contain", "baz");
});
