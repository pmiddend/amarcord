customElements.define('uglymol-viewer',
    class extends HTMLElement {
        // things required by Custom Elements
        constructor() {
            super();
            this.attachShadow({mode: "open"});
            this.constructed = false;

            if (this.getAttribute("idprefix") === null) {
                return
            }
            this.constructed = true;
            this.construct();
        }

        construct() {
            if (this.getAttribute("idprefix") === null) {
                return
            }
            if (this.constructed || this.getAttribute("idprefix") === null)
                return;
            this.constructed = true;
            const wrapper = document.createElement("div");
            const viewer = document.createElement("div");
            const idprefix = this.getAttribute('idprefix');
            viewer.setAttribute("id", idprefix + "viewer")
            viewer.setAttribute("class", "uglymol-viewer")
            wrapper.appendChild(viewer);
            const hud = document.createElement("div");
            hud.setAttribute("id", idprefix + "hud")
            hud.setAttribute("class", "uglymol-hud")
            wrapper.appendChild(hud);
            const help = document.createElement("div");
            help.setAttribute("id", idprefix + "help")
            help.setAttribute("class", "uglymol-help")
            wrapper.appendChild(help);
            const style = document.createElement("style");
            style.textContent = `.uglymol-viewer {
                width: 60em;
                height: 40em;
                background-color: black;
            }

            .uglymol-hud {
              display: none;
            }
            .uglymol-help {
              display: none;
            }
            /*
            .uglymol-hud {
                font-size: 15px;
                color: #ddd;
                background-color: rgba(0, 0, 0, 0.6);
                text-align: center;
                position: absolute;
                top: 10px;
                left: 50%;
                transform: translateX(-50%);
                padding: 2px 8px;
                border-radius: 5px;
                z-index: 9;
                white-space: pre-line;
            }

            .uglymol-hud u {
                padding: 0 8px;
                text-decoration: none;
                border: solid;
                border-width: 1px 0;
            }

            .uglymol-hud s {
                padding: 0 8px;
                text-decoration: none;
                opacity: 0.5;
            }

            .uglymol-help {
                display: none;
                font-size: 16px;
                color: #eee;
                background-color: rgba(0, 0, 0, 0.7);
                position: absolute;
                left: 20px;
                top: 50%;
                transform: translateY(-50%);
                cursor: default;
                padding: 5px;
                border-radius: 5px;
                z-index: 9;
                white-space: pre-line;
            }
            */
            `;

            this.shadowRoot.append(style, wrapper);
        }

        connectedCallback() {
            this.setContent();
        }

	disconnectedCallback() {
	    
	}

        setContent() {
            if (this.getAttribute('idprefix') === null) {
                return;
            }

            this.construct();

            const pdbId = this.getAttribute('pdbid');
            const mtzId = this.getAttribute('mtzid');
            const idprefix = this.getAttribute('idprefix');

            const V = new UM.Viewer({shadowRoot: this.shadowRoot, viewer: idprefix + "viewer", hud: idprefix + "hud", help: idprefix + "help"});
            V.load_pdb("api/files/" + pdbId);
            GemmiMtz().then(function (Module) {
                UM.load_maps_from_mtz(Module, V, "api/files/" + mtzId, null);
            });
        }

        attributeChangedCallback() {
            this.setContent();
        }

        static get observedAttributes() {
            return ['pdbid', 'mtzid', 'idprefix'];
        }
    }
);
