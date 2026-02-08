# KEHRNEL OpenEHR API Documentation

A comprehensive Next.js frontend for documenting and exploring the KEHRNEL OpenEHR API implementation built with Python, FastAPI, and MongoDB.

## 🌟 Features

- **Interactive API Explorer**: Test API endpoints with live examples and responses
- **Comprehensive Documentation**: Detailed guides for all OpenEHR resources
- **Modern UI**: Clean, responsive design with Tailwind CSS
- **Dynamic Content**: Easily extensible structure for new features
- **Syntax Highlighting**: Code examples with copy-to-clipboard functionality
- **TypeScript**: Full type safety and better development experience

## 🏗️ Architecture

The documentation covers the following OpenEHR API resources:

### Core Resources
- **AQL (Archetype Query Language)**: Query operations and transformations
- **Composition**: Clinical document management
- **Contribution**: Version control and audit trails  
- **EHR**: Electronic Health Record operations
- **EHR Status**: Record status and metadata management
- **Directory**: Folder structure management (future implementation)

### Additional Resources
- **Ingest**: Data ingestion operations
- **Synthetic**: Synthetic data generation

## 🚀 Getting Started

### Prerequisites
- Node.js 18+ 
- npm, yarn, pnpm, or bun

### Installation

1. Clone the repository:
```bash
git clone https://github.com/Paco-Mateu/kehrnel.git
cd kehrnel/frontend
```

2. Install dependencies:
```bash
npm install
# or
yarn install
# or
pnpm install
# or
bun install
```

3. Start the development server:
```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

4. Open [http://localhost:3000](http://localhost:3000) in your browser.

## 📁 Project Structure

```
src/
├── app/
│   ├── layout.tsx          # Root layout with navigation
│   ├── page.tsx            # Home page
│   ├── api/
│   │   └── page.tsx        # API Explorer
│   └── docs/
│       └── aql/
│           └── page.tsx    # AQL documentation
├── components/
│   ├── Navigation.tsx      # Header navigation component
│   ├── APIExplorer.tsx     # Interactive API documentation
│   └── CodeBlock.tsx       # Syntax highlighted code blocks
└── ...
```

## 🧭 Navigation Structure

### Main Navigation
- **KEHRNEL**: Links to the GitHub repository
- **Docs**: Dropdown with documentation for each resource
- **OpenEHR API**: Interactive API explorer with live testing
- **Data Lab**: Link to the Data Lab platform (external)

### Documentation Sections
- **AQL**: Complete guide to Archetype Query Language
- **Composition**: Clinical document operations
- **Contribution**: Version control and audit
- **EHR**: Patient record management  
- **EHR Status**: Record status operations
- **Directory**: Folder management (coming soon)

## 🔧 Development

### Available Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run start` - Start production server
- `npm run lint` - Run ESLint

### Adding New Documentation

1. Create a new page in `src/app/docs/[resource]/page.tsx`
2. Add the route to the navigation dropdown in `src/components/Navigation.tsx`
3. Add API operations to `src/components/APIExplorer.tsx`

### Customizing the API Explorer

The API Explorer is driven by data structures in `APIExplorer.tsx`. To add new resources or operations:

1. Update the `apiResources` array with new resource definitions
2. Add operation details including parameters, request/response examples
3. The component will automatically render the new content

## 🎨 Styling

This project uses [Tailwind CSS](https://tailwindcss.com/) for styling:

- Responsive design with mobile-first approach
- Consistent color scheme and typography
- Custom components for code blocks and API documentation
- Dark mode support (can be added)

## 📖 OpenEHR Compliance

This documentation follows OpenEHR specifications and standards:

- Official OpenEHR resource definitions
- Compliant AQL syntax and operations
- Standard archetype patterns and structures
- Proper terminology and naming conventions

## 🔗 Related Projects

- **KEHRNEL Backend**: Python/FastAPI OpenEHR API implementation
- **Data Lab**: Analytics platform for OpenEHR data exploration

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Commit your changes: `git commit -am 'Add new feature'`
4. Push to the branch: `git push origin feature/new-feature`
5. Submit a pull request

## 📝 License

This project is part of the KEHRNEL OpenEHR implementation. See the main repository for license information.

## 🆘 Support

- Check the documentation sections for detailed guides
- Use the API Explorer to test endpoints interactively
- Refer to [OpenEHR specifications](https://specifications.openehr.org/) for standard definitions
- Open issues in the GitHub repository for bugs or feature requests
